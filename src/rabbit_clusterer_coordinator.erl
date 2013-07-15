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
    State1 = #state { node_id = NodeId } =
        case rabbit_clusterer_utils:ensure_node_id() of
            {ok, NodeId1} -> State #state { node_id = NodeId1 };
            Err1         -> reply_awaiting(Err1, State)
        end,
    case choose_config() of
        {undefined, undefined} ->
            %% No config at all, 'join' the default.
            {ok, begin_transition(
                   rabbit_clusterer_join,
                   rabbit_clusterer_utils:default_config(NodeId), State1)};
        {Config, Config} ->
            %% Configuration has not changed. We think.
            {ok, begin_transition(rabbit_clusterer_rejoin, Config, State1)};
        {NewConfig, OldConfig} ->
            %% New cluster config has been applied
            NewConfig1 =
                rabbit_clusterer_utils:merge_node_id_maps(NewConfig, OldConfig),
            {ok, begin_transition(rabbit_clusterer_join, NewConfig1, State1)}
    end.

handle_call(await_coordination, From,
            State = #state { status = undefined, awaiting_cluster = AC }) ->
    {noreply, State #state { awaiting_cluster = [From | AC] }};
handle_call(await_coordination, _From, State = #state { status = Status }) ->
    {reply, Status, State};
handle_call({request_status, Node, NodeID}, From,
            State = #state { config             = Config,
                             transitioner       = TModule,
                             transitioner_state = TState,
                             status             = Status }) ->
    ResultStatus = case Status of
                       ok -> ok;
                       _  -> TModule
                   end,
    {NodeIDChanged, Config1} =
        rabbit_clusterer_utils:add_node_id(Node, NodeID, Config),
    gen_server:reply(From, {Config1, ResultStatus}),
    State1 = State #state { config = Config1 },
    State2 = case TModule of
                 undefined ->
                     reset_shutdown(State1);
                 _ when NodeIDChanged ->
                     process_transitioner_response(
                       TModule:event({node_reset, Node}, TState), State1);
                 _ ->
                     State1
             end,
    {noreply, State2};
handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast({comms, Comms, Result},
            State = #state { comms              = Comms,
                             transitioner       = TModule,
                             transitioner_state = TState })
  when TModule =/= undefined ->
    {noreply, process_transitioner_response(
                TModule:event({comms, Result}, TState), State)};
handle_cast({comms, _Comms, _Result}, State) ->
    %% Must be from an old comms. Ignore silently by design.
    {noreply, State};
handle_cast({new_config, ConfigNew}, State = #state { config = ConfigOld }) ->
    ConfigNew1 = rabbit_clusterer_utils:merge_node_id_maps(ConfigNew, ConfigOld),
    {noreply, begin_transition(rabbit_clusterer_join, ConfigNew1, State)};
handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info({shutdown, Ref},
            State = #state { status = undefined,
                             transitioner = undefined,
                             transitioner_state = {shutdown, Ref} }) ->
    {stop, normal,
     reply_awaiting(shutdown, State #state { transitioner = undefined })};
handle_info({shutdown, _Ref}, State) ->
    %% Something saved us in the meanwhilst!
    {noreply, State};
handle_info({transitioner_delay, _Event},
            State = #state { transitioner = undefined }) ->
    {noreply, State};
handle_info({transitioner_delay, Event},
            State = #state { transitioner       = TModule,
                             transitioner_state = TState }) ->
    {noreply, process_transitioner_response(TModule:event(Event, TState), State)};
handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------
%% Cluster config loading and selection
%%----------------------------------------------------------------------------

internal_config_path() ->
    rabbit_mnesia:dir() ++ "-cluster.config".

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
            [ExternalConfig, InternalConfig] =
                lists:map(fun rabbit_clusterer_utils:proplist_config_to_record/1,
                          [ExternalProplist, InternalProplist]),
            %% We deliberately require > and not >=. I.e. if a user
            %% provides a config, it must have a strictly greater
            %% version than our current config in order to be valid.
            case rabbit_clusterer_utils:compare_configs(ExternalConfig,
                                                        InternalConfig) of
                gt -> {ExternalConfig, InternalConfig};
                _  -> {InternalConfig, InternalConfig}
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
process_transitioner_response(shutdown, State = #state { config = Config }) ->
    %% If we've had a config applied to us that tells us to shutdown,
    %% we must record that config, otherwise we can later be restarted
    %% and try to start up with an outdated config.
    ok = write_internal_config(Config),
    {ok, State1} = stop_comms(State),
    reset_shutdown(
      State1 #state { transitioner       = undefined,
                      transitioner_state = {shutdown, undefined} });
process_transitioner_response({success, ConfigNew},
                              State = #state { config = ConfigOld }) ->
    %% Just to be safe, merge through the node_id_maps
    ConfigNew1 = rabbit_clusterer_utils:merge_node_id_maps(ConfigNew, ConfigOld),
    ok = write_internal_config(ConfigNew1),
    {ok, State1} = stop_comms(State #state { transitioner = undefined,
                                             transitioner_state = undefined,
                                             config = ConfigNew1 }),
    reply_awaiting_ok(State1);
process_transitioner_response({config_changed, ConfigNew},
                              State = #state { config = ConfigOld }) ->
    %% Just to be safe, merge through the node_id_maps
    ConfigNew1 = rabbit_clusterer_utils:merge_node_id_maps(ConfigNew, ConfigOld),
    %% If the config has changed then we must now be joining a new
    %% config
    begin_transition(rabbit_clusterer_join, ConfigNew1, State);
process_transitioner_response({sleep, Delay, Event, TState}, State) ->
    erlang:send_after(Delay, self(), {transitioner_delay, Event}),
    State #state { transitioner_state = TState }.


fresh_comms(State) ->
    {ok, State1} = stop_comms(State),
    {ok, Token} = rabbit_clusterer_comms_sup:start_comms(),
    {ok, Token, State1 #state { comms = Token }}.

stop_comms(State = #state { comms = undefined }) ->
    {ok, State};
stop_comms(State = #state { comms = Token }) ->
    ok = rabbit_clusterer_comms:stop(Token),
    {ok, State #state { comms = undefined }}.

reset_shutdown(State = #state { config = #config { shutdown_timeout = Timeout },
                                transitioner = undefined,
                                transitioner_state = {shutdown, _Ref} }) ->
    case Timeout of
        infinity ->
            State  #state { transitioner_state = undefined };
        _ ->
            Ref = make_ref(),
            erlang:send_after(Timeout*1000, self(), {shutdown, Ref}),
            State #state { transitioner_state = {shutdown, Ref} }
    end;
reset_shutdown(State) ->
    State.
