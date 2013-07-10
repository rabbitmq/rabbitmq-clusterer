-module(rabbit_clusterer_coordinator).

-behaviour(gen_server).

-export([await_coordination/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { status,
                 awaiting_cluster,
                 node_id,
                 config,
                 transitioner,
                 transitioner_state,
                 comms
               }).

-include("rabbit_clusterer.hrl").

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

await_coordination() ->
    gen_server:call(?SERVER, await_coordination, infinity).

%%----------------------------------------------------------------------------

init([]) ->
    ok = rabbit_mnesia:ensure_mnesia_dir(),
    State = #state { status             = undefined,
                     awaiting_cluster   = [],
                     node_id            = undefined,
                     config             = undefined,
                     transitioner       = undefined,
                     transitioner_state = undefined,
                     comms              = undefined },
    State1 = case ensure_node_id() of
                 {ok, NodeId} -> State #state { node_id = NodeId };
                 Err1         -> reply_awaiting(Err1, State)
             end,
    case choose_config() of
        {undefined, undefined} ->
            %% No config at all, 'join' the default.
            {ok, begin_transition(
                   rabbit_clusterer_join,
                   rabbit_clusterer_utils:default_config(), State1)};
        {Config, Config} ->
            %% Configuration has not changed. We think.
            {ok, begin_transition(rabbit_clusterer_rejoin, Config, State1)};
        {NewConfig, _} ->
            %% New cluster config has been applied
            {ok, begin_transition(rabbit_clusterer_join, NewConfig, State1)}
    end.

handle_call(await_coordination, From,
            State = #state { status = undefined, awaiting_cluster = AC }) ->
    {noreply, State #state { awaiting_cluster = [From | AC] }};
handle_call(await_coordination, _From, State = #state { status = Status }) ->
    {reply, Status, State};
handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info({shutdown, Ref}, State = #state { status = undefined,
                                              transitioner = shutdown,
                                              transitioner_state = Ref }) ->
    {stop, normal,
     reply_awaiting(shutdown, State #state { transitioner = undefined })};
handle_info({shutdown, _Ref}, State) ->
    {noreply, State};
handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------
%% Node ID
%%----------------------------------------------------------------------------

node_id_file_path() ->
    filename:join(rabbit_mnesia:dir(), "node_id").

ensure_node_id() ->
    case rabbit_file:read_term_file(node_id_file_path()) of
        {ok, [NodeId]}    -> {ok, NodeId};
        {error, enoent}   -> create_node_id();
        {error, _E} = Err -> Err
    end.

create_node_id() ->
    %% We can't use rabbit_guid here because it hasn't been started at
    %% this stage. In reality, this isn't a massive problem: the fact
    %% we need to create a node_id implies that we're a fresh node, so
    %% the guid serial will be 0 anyway.
    NodeID = erlang:md5(term_to_binary({node(), make_ref()})),
    case rabbit_file:write_term_file(node_id_file_path(), [NodeID]) of
        ok                -> {ok, NodeID};
        {error, _E} = Err -> Err
    end.


%%----------------------------------------------------------------------------
%% Cluster config loading and selection
%%----------------------------------------------------------------------------

internal_config_path() ->
    filename:join(rabbit_mnesia:dir(), "cluster.config").

external_config_path() ->
    application:get_env(rabbit, cluster_config).

choose_config() ->
    ExternalProplist =
        case external_config_path() of
            undefined ->
                undefined;
            {ok, ExternalPath} ->
                case rabbit_file:read_term_file(ExternalPath) of
                    {error, enoent}           -> undefined;
                    {ok, [ExternalProplist1]} -> ExternalProplist1
                end
        end,
    InternalProplist = case rabbit_file:read_term_file(internal_config_path()) of
                           {error, enoent}          -> undefined;
                           {ok, [InternalProplist1]} -> InternalProplist1
                       end,
    case {ExternalProplist, InternalProplist} of
        {undefined, undefined} ->
            {undefined, undefined};
        {undefined, Proplist} ->
            Config = rabbit_clusterer_utils:proplist_config_to_record(Proplist),
            {Config, Config};
        {Proplist, undefined} ->
            {rabbit_clusterer_utils:proplist_config_to_record(Proplist), undefined};
        _ ->
            [ExternalConfig = #config { version = ExternalV,
                                        minor_version = ExternalMV },
             InternalConfig = #config { version = InternalV,
                                        minor_version = InternalMV }] =
                [rabbit_clusterer_utils:proplist_config_to_record(Proplist)
                 || Proplist <- [InternalProplist, ExternalProplist]],
            %% We deliberately require > and not >=. I.e. if a user
            %% provides a config, it must have a strictly greater
            %% version than our current config in order to be valid.
            case {ExternalV, ExternalMV} > {InternalV, InternalMV} of
                true  -> {ExternalConfig, InternalConfig};
                false -> {InternalConfig, InternalConfig}
            end
    end.

write_internal_config(Config) ->
    Proplist = rabbit_clusterer_utils:record_config_to_proplist(Config),
    ok = rabbit_file:write_term_file(internal_config_path(), [Proplist]).

%%----------------------------------------------------------------------------
%% Signalling to waiting processes
%%----------------------------------------------------------------------------

reply_awaiting_ok(State) ->
    reply_awaiting(ok, State).

reply_awaiting(Term, State = #state { awaiting_cluster = AC,
                                      status = undefined }) ->
    [gen_server:reply(From, Term) || From <- AC],
    State #state { awaiting_cluster = [], status = Term }.


%%----------------------------------------------------------------------------
%% Changing cluster config
%%----------------------------------------------------------------------------

begin_transition(TModule, Config, State = #state { node_id = NodeID }) ->
    {ok, Comms, State1} = fresh_comms(State),
    process_transitioner_response(TModule:init(Config, NodeID, Comms),
                                  State1 #state { status       = undefined,
                                                  transitioner = TModule,
                                                  config       = Config }).


process_transitioner_response({continue, TState}, State) ->
    State #state { transitioner_state = TState };
process_transitioner_response(
  {shutdown, Config = #config { shutdown_timeout = Timeout }}, State) ->
    %% If we've had a config applied to us that tells us to shutdown,
    %% we must record that config, otherwise we can later be restarted
    %% and try to start up with an outdated config.
    ok = write_internal_config(Config),
    {ok, State1} = stop_comms(State),
    case Timeout of
        infinity ->
            State1 #state { transitioner = undefined,
                            transitioner_state = undefined };
        _ ->
            Ref = make_ref(),
            erlang:send_after(Timeout*1000, self(), {shutdown, Ref}),
            State1 #state { transitioner       = shutdown,
                            transitioner_state = Ref }
    end;
process_transitioner_response({success, Config}, State) ->
    ok = write_internal_config(Config),
    {ok, State1} = stop_comms(State #state { transitioner = undefined,
                                             transitioner_state = undefined }),
    reply_awaiting_ok(State1);
process_transitioner_response({config_changed, Config}, State) ->
    %% If the config has changed then we must now be joining a new
    %% config
    begin_transition(rabbit_clusterer_join, Config, State).

fresh_comms(State) ->
    {ok, State1} = stop_comms(State),
    {ok, Token} = rabbit_clusterer_comms_sup:start_comms(),
    {ok, Token, State1 #state { comms = Token }}.

stop_comms(State = #state { comms = undefined }) ->
    {ok, State};
stop_comms(State = #state { comms = Token }) ->
    ok = rabbit_clusterer_comms:stop(Token),
    {ok, State #state { comms = undefined }}.
