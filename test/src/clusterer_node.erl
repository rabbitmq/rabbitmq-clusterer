-module(clusterer_node).

-export([observe_state/1, all_stable/1,
         start_link/2, delete/1,
         reset/1, start/1, start_with_config/2, apply_config/2, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("clusterer_test.hrl").

-record(state, { name, name_str, port }).

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

observe_state(Pids) ->
    todo.

all_stable(Pids) ->
    todo.

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

start_link(Name, Port) when is_atom(Name) andalso is_integer(Port) ->
    gen_server:start_link(?MODULE, [Name, Port], []).

delete(Pid) -> gen_server:cast(Pid, delete).

reset(Pid)  -> gen_server:cast(Pid, reset).

start(Pid)  -> gen_server:cast(Pid, start).

start_with_config(Pid, Config) ->
    gen_server:cast(Pid, {start_with_config, Config}).

apply_config(Pid, Config) -> gen_server:cast(Pid, {apply_config, Config}).

stop(Pid) -> gen_server:cast(Pid, stop).

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

init([Name, Port]) ->
    {ok, #state { name     = Name,
                  name_str = atom_to_list(Name),
                  port     = rabbit_misc:format("~p", [Port]) }}.

handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast(delete, State = #state { name = Name }) ->
    pang = net_adm:ping(Name), %% ASSERTION
    ok = make_cmd("cleandb", "", State),
    ok = delete_internal_cluster_config(State),
    {stop, normal, State};
handle_cast(reset, State = #state { name = Name }) ->
    pang = net_adm:ping(Name), %% ASSERTION
    ok = make_cmd("cleandb", "", State),
    ok = delete_internal_cluster_config(State),
    {noreply, State};
handle_cast(start, State = #state { name = Name }) ->
    pang = net_adm:ping(Name), %% ASSERTION
    ok = make_bg_cmd("run", "-noinput", State),
    {noreply, State};
handle_cast(stop, State) ->
    %% It could have been in pending_shutdown and have died off.
    %% However, make stop-node doesn't error if the node's not
    %% running.
    ok = make_cmd("stop-node", "", State),
    {noreply, State};
handle_cast({start_with_config, Config}, State = #state { name_str = NameStr }) ->
    ok = store_external(NameStr, Config),
    ok = make_bg_cmd("run", "-noinput -rabbitmq_clusterer config \\\\\\\"" ++
                         external_config_file(NameStr) ++ "\\\\\\\"",
                     State),
    {noreply, State};
handle_cast({apply_config, Config}, State = #state { name     = Name,
                                                     name_str = NameStr }) ->
    case net_adm:ping(Name) of
        pong ->
            ok = store_external(NameStr, Config),
            ok = ctl("eval 'rabbit_clusterer:apply_config(\"" ++
                         external_config_file(NameStr) ++ "\").'", State),
            {noreply, State};
        pang ->
            handle_cast({start_with_config, Config}, State)
    end;
handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

makefile_dir() ->
    filename:join(filename:dirname(code:which(rabbit)), "..").

mnesia_dir(NameStr) when is_list(NameStr) ->
    DirName = "rabbitmq-" ++ NameStr ++ "-mnesia",
    case {os:getenv("RABBITMQ_MNESIA_DIR"), os:getenv("TMPDIR")} of
        {false, false } -> filename:join("/tmp", DirName);
        {false, TmpDir} -> filename:join(TmpDir, DirName);
        {Dir,   _     } -> Dir
    end.

external_config_file(NameStr) when is_list(NameStr) ->
    mnesia_dir(NameStr) ++ "-external-cluster.config".

ctl(Action, #state { name_str = NameStr }) ->
    Cmd = lists:flatten([filename:join(makefile_dir(), "scripts/rabbitmqctl"),
                         " -n '",
                         NameStr, "' ", Action, " ; echo $?"]),
    Res = os:cmd(Cmd),
    LastLine = hd(lists:reverse(string:tokens(Res, "\n"))),
    "0" = LastLine, %% ASSERTION
    ok.

make_cmd(Action, StartArgs, #state { name_str = NameStr, port = Port }) ->
    Cmd = lists:flatten(["RABBITMQ_NODENAME=",
                         NameStr,
                         " RABBITMQ_NODE_PORT=",
                         Port,
                         " RABBITMQ_SERVER_START_ARGS=\"",
                         StartArgs,
                         "\" make -C ",
                         makefile_dir(),
                         " ",
                         Action,
                         " ; echo $?"]),
    Res = os:cmd(Cmd),
    LastLine = hd(lists:reverse(string:tokens(Res, "\n"))),
    "0" = LastLine, %% ASSERTION
    ok.

make_bg_cmd(Action, StartArgs, #state { name_str = NameStr, port = Port }) ->
    Cmd = lists:flatten(["RABBITMQ_NODENAME=",
                         NameStr,
                         " RABBITMQ_NODE_PORT=",
                         Port,
                         " RABBITMQ_SERVER_START_ARGS=\"",
                         StartArgs,
                         "\" setsid make -C ",
                         makefile_dir(),
                         " ",
                         Action,
                         " >/dev/null 2>/dev/null &"]),
    os:cmd(Cmd),
    ok.


delete_internal_cluster_config(#state { name_str = NameStr }) ->
    case file:delete(mnesia_dir(NameStr) ++ "-cluster.config") of
        ok              -> ok;
        {error, enoent} -> ok;
        Err             -> Err
    end.

store_external(NameStr, Config) when is_list(NameStr) ->
    ok = rabbit_file:write_term_file(external_config_file(NameStr),
                                     [to_proplist(Config)]).

field_fold(Fun, Init) ->
    {_Pos, Res} = lists:foldl(fun (FieldName, {Pos, Acc}) ->
                                      {Pos + 1, Fun(FieldName, Pos, Acc)}
                              end, {2, Init}, record_info(fields, config)),
    Res.

to_proplist(Config = #config {}) ->
    field_fold(fun (FieldName, Pos, ProplistN) ->
                       [{FieldName, element(Pos, Config)} | ProplistN]
               end, []).
