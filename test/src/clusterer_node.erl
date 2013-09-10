-module(clusterer_node).

-export([observe_stable_state/1,
         start_link/2, delete/1,
         reset/1, start/1, start_with_config/2, apply_config/2, stop/1,
         exit/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("clusterer_test.hrl").

-record(node_state, { name, name_str, port }).

-define(IS_NODE_OFF(R), R =:= noconnection; R =:= nodedown; R =:= noproc).
-define(SLEEP, timer:sleep(250)).

%%----------------------------------------------------------------------------

observe_stable_state([]) ->
    {stable, orddict:new()};
observe_stable_state(Pids) ->
    Self = self(),
    Ref = make_ref(),
    [gen_server:cast(Pid, {stable_state, Ref, Self}) || Pid <- Pids],
    Results = [receive
                   {stable_state, Ref, Name, Result} ->
                       {Name, Result}
               end || _Pid <- Pids],
    case [Name || {Name, false} <- Results] of
        [] -> {stable, orddict:from_list(lists:usort(Results))};
        _  -> not_stable
    end.

%%----------------------------------------------------------------------------

start_link(Name, Port) when is_atom(Name) andalso is_integer(Port) ->
    gen_server:start_link(?MODULE, [Name, Port], []).

delete(Pid) -> gen_server:cast(Pid, delete).

reset(Pid)  -> gen_server:cast(Pid, reset).

start(Pid)  -> gen_server:cast(Pid, start).

start_with_config(Pid, Config) ->
    gen_server:cast(Pid, {start_with_config, Config}).

apply_config(Pid, Config) -> gen_server:cast(Pid, {apply_config, Config}).

stop(Pid) -> gen_server:cast(Pid, stop).

exit(Pid) -> gen_server:call(Pid, exit, infinity).

%%----------------------------------------------------------------------------

init([Name, Port]) ->
    State = #node_state { name     = Name,
                          name_str = atom_to_list(Name),
                          port     = rabbit_misc:format("~p", [Port]) },
    pang = net_adm:ping(Name), %% ASSERTION
    ok = clean_db(State),
    {ok, State}.

handle_call(exit, _From, State = #node_state { name = Name }) ->
    ok = run_cmd("stop-node", State),
    ok = await_death(Name),
    ok = clean_db(State),
    {stop, normal, ok, State};
handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast(delete, State = #node_state { name = Name }) ->
    pang = net_adm:ping(Name), %% ASSERTION
    ok = clean_db(State),
    {stop, normal, State};
handle_cast(reset, State = #node_state { name = Name }) ->
    pang = net_adm:ping(Name), %% ASSERTION
    ok = clean_db(State),
    {noreply, State};
handle_cast(start, State = #node_state { name = Name }) ->
    pang = net_adm:ping(Name), %% ASSERTION
    ok = run_bg_cmd("run", "-noinput", State),
    ok = await_life(Name),
    {noreply, State};
handle_cast(stop, State = #node_state { name = Name }) ->
    pong = net_adm:ping(Name),
    ok = run_cmd("stop-node", State),
    ok = await_death(Name),
    {noreply, State};
handle_cast({start_with_config, Config},
            State = #node_state { name = Name, name_str = NameStr }) ->
    ok = store_external_cluster_config(NameStr, Config),
    ok = run_bg_cmd("run", "-rabbitmq_clusterer config \\\\\\\"" ++
                         external_config_file(NameStr) ++ "\\\\\\\" -noinput",
                     State),
    ok = await_life(Name),
    {noreply, State};
handle_cast({apply_config, Config},
            State = #node_state { name = Name, name_str = NameStr }) ->
    pong = net_adm:ping(Name),
    ok = store_external_cluster_config(NameStr, Config),
    ok = ctl("eval 'rabbit_clusterer:apply_config(\"" ++
                 external_config_file(NameStr) ++ "\").'", State),
    {noreply, State};
handle_cast({stable_state, Ref, From},
            State = #node_state { name = Name, name_str = NameStr}) ->
    Result =
        try
            case rabbit_clusterer_coordinator:request_status(Name) of
                preboot                      -> false;
                {Config,  {transitioner, _}} -> {ready, convert(Config)};
                {_Config, booting}           -> false;
                { Config, ready}             -> {ready, convert(Config)};
                { Config, pending_shutdown}  -> {pending_shutdown,
                                                 convert(Config)}
            end
        catch
            exit:{R, _}      when ?IS_NODE_OFF(R) ->
                case is_reset(NameStr) of
                    true  -> reset;
                    false -> off
                end;
            exit:{{R, _}, _} when ?IS_NODE_OFF(R) ->
                case is_reset(NameStr) of
                    true  -> reset;
                    false -> off
                end;
            _Class:_Reason ->
                false
        end,
    From ! {stable_state, Ref, Name, Result},
    {noreply, State};
handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

await_death(Name) ->
    await(Name, pong, pang).

await_life(Name) ->
    await(Name, pang, pong).

await(Name, Again, Return) ->
    case net_adm:ping(Name) of
        Again  -> ?SLEEP,
                  await(Name, Again, Return);
        Return -> ok
    end.

convert(ClustererConfig) ->
    Version = rabbit_clusterer_config:version(ClustererConfig),
    NodesNames = rabbit_clusterer_config:nodenames(ClustererConfig),
    DiscNodeNames = rabbit_clusterer_config:disc_nodenames(ClustererConfig),
    RamNodeNames = NodesNames -- DiscNodeNames,
    Gospel = rabbit_clusterer_config:gospel(ClustererConfig),
    ShutdownTimeout = rabbit_clusterer_config:shutdown_timeout(ClustererConfig),
    #config { version          = Version,
              nodes            = orddict:from_list(
                                   [{Name, disc} || Name <- DiscNodeNames] ++
                                       [{Name, ram} || Name <- RamNodeNames]),
              gospel           = Gospel,
              shutdown_timeout = ShutdownTimeout }.

makefile_dir() ->
    filename:join(filename:dirname(code:which(rabbit)), "..").

mnesia_dir(NameStr) when is_list(NameStr) ->
    DirName = "rabbitmq-" ++ NameStr ++ "-mnesia",
    case {os:getenv("RABBITMQ_MNESIA_DIR"), os:getenv("TMPDIR")} of
        {false, false } -> filename:join("/tmp", DirName);
        {false, TmpDir} -> filename:join(TmpDir, DirName);
        {Dir,   _     } -> Dir
    end.

is_reset(NameStr) when is_list(NameStr) ->
    Dir = mnesia_dir(NameStr),
    case file:list_dir(Dir) of
        {error, enoent} -> true;
        {ok,    []    } -> true;
        {ok,    _     } -> false
    end.

external_config_file(NameStr) when is_list(NameStr) ->
    mnesia_dir(NameStr) ++ "-external-cluster.config".

ctl(Action, #node_state { name_str = NameStr }) ->
    Cmd = lists:flatten([filename:join(makefile_dir(), "scripts/rabbitmqctl"),
                         " -n '",
                         NameStr, "' ", Action, " ; echo $?"]),
    Res = os:cmd(Cmd),
    LastLine = hd(lists:reverse(string:tokens(Res, "\n"))),
    "0" = LastLine, %% ASSERTION
    ok.

run_cmd(Action, #node_state { name_str = NameStr, port = Port }) ->
    Cmd = lists:flatten(["RABBITMQ_NODENAME=",
                         NameStr,
                         " RABBITMQ_NODE_PORT=",
                         Port,
                         " make -C ",
                         makefile_dir(),
                         " ",
                         Action,
                         " ; echo $?"]),
    Res = os:cmd(Cmd),
    LastLine = hd(lists:reverse(string:tokens(Res, "\n"))),
    "0" = LastLine, %% ASSERTION
    ok.

run_bg_cmd(Action, StartArgs, #node_state { name_str = NameStr, port = Port }) ->
    Log = mnesia_dir(NameStr),
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
                         " 1>> ",
                         Log,
                         "-stdout.log 2>> ",
                         Log,
                         "-stderr.log &"]),
    os:cmd(Cmd),
    ok.

clean_db(State = #node_state { name_str = NameStr }) ->
    ok = run_cmd("cleandb", State),
    case file:delete(mnesia_dir(NameStr) ++ "-cluster.config") of
        ok              -> ok;
        {error, enoent} -> ok;
        Err             -> Err
    end.

store_external_cluster_config(NameStr, Config) when is_list(NameStr) ->
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
