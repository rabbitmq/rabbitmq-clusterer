-module(rabbit_clusterer_coordinator).

-behaviour(gen_server).

-export([await_coordination/0, ready_to_cluster/0]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(IS_TRANSITIONER(X),
        (X =:= rabbit_clusterer_join orelse X =:= rabbit_clusterer_rejoin)).

-record(state, { status,
                 boot_blocker,
                 config,
                 transitioner_state,
                 comms
               }).

-include("rabbit_clusterer.hrl").

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

await_coordination() ->
    gen_server:call(?SERVER, await_coordination, infinity).

ready_to_cluster() ->
    gen_server:cast(?SERVER, ready_to_cluster),
    ok.

%%----------------------------------------------------------------------------

init([]) ->
    ok = rabbit_mnesia:ensure_mnesia_dir(),
    {ok, #state { status             = preboot,
                  boot_blocker       = undefined,
                  config             = undefined,
                  transitioner_state = undefined,
                  comms              = undefined }}.

handle_call(await_coordination, From,
            State = #state { status       = preboot,
                             boot_blocker = undefined }) ->
    %% We deliberately don't start doing any clustering work until we
    %% get this call. This is because we need to wait for the boot
    %% step to make the call as only by then can we be sure that
    %% things like the file_handle_cache have been started up.
    {noreply, begin_clustering(State #state { boot_blocker = From })};
handle_call({request_status, _Node, _NodeID}, _From,
            State = #state { status = preboot }) ->
    %% If status = preboot then we have the situation that a remote
    %% node is contacting us before we've even started reading in our
    %% cluster configs. We need to ignore them. They'll either wait
    %% for us, or they'll start up and bring us in later on anyway.
    {reply, preboot, State};
handle_call({request_status, Node, NodeID}, _From,
            State = #state { transitioner_state = TState,
                             status    = Status = {transitioner, TModule} }) ->
    {Config, State1} =
        process_transitioner_response(
          TModule:event({request_config, Node, NodeID}, TState), State),
    {reply, {Config, Status}, State1};
handle_call({request_status, Node, NodeID}, _From,
            State = #state { config = Config, status = Status }) ->
    {_NodeIDChanged, Config1} =
        rabbit_clusterer_utils:add_node_id(Node, NodeID, Config),
    {reply, {Config1, Status}, reset_shutdown(State #state { config = Config1 })};
handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast({comms, Comms, Result},
            State = #state { comms              = Comms,
                             status             = Status,
                             transitioner_state = TState }) ->
    {noreply,
     case Status of
         {transitioner, TModule} ->
             process_transitioner_response(
               TModule:event({comms, Result}, TState), State);
         _ -> %% Must be from an old comms. Ignore silently by design.
             State
     end};
handle_cast({new_config, _ConfigNew}, State = #state { status = preboot }) ->
    %% ignore it, we'll recover later
    {noreply, State};
handle_cast({new_config, ConfigNew}, State = #state { status = Status,
                                                      config = ConfigOld })
  when Status =:= booting orelse Status =:= ready ->
    ConfigNew1 = rabbit_clusterer_utils:merge_configs(ConfigNew, ConfigOld),
    case rabbit_clusterer_utils:detect_melisma(ConfigNew1, ConfigOld) of
        true ->
            %% It's a "fully compatible" change. We will need to
            %% update the nodes we're monitoring, and we might
            %% actually need to shutdown, but basically, very minor
            %% changes to us - we shouldn't need to restart rabbit.
            %% TODO: the above
            {noreply, State #state { config = ConfigNew1 }};
        false ->
            %% Ahh, we will need to reboot here. TODO
            io:format("Reboot required...~n"),
            {stop, normal, State}
    end;
handle_cast({new_config, ConfigNew}, State) ->
    %% status =:= pending_shutdown orelse status =:= {transitioner,_}
    %% In either case we consider the transition from our last active
    %% config, *not* the config we're currently trying to transition
    %% to (if {transitioner,_}).
    {noreply, begin_transition(ConfigNew, State)};
handle_cast(ready_to_cluster, State = #state { status = booting }) ->
    {noreply, set_status(ready, State)};
handle_cast(ready_to_cluster, State) ->
    %% Complex race: we thought we were booting up and so status would
    %% have been 'booting' at some point, but since then someone else
    %% has provided a new config (and we decided not to reboot - TODO
    %% - is this possible?). Consequenctly rabbit has now passed the
    %% mnesia boot step and is continuing to boot up. Presumably we
    %% actually just want to reboot in the future after rabbit has
    %% finished (erroneously) booting...
    {noreply, State};
handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info({shutdown, Ref},
            State = #state { status = pending_shutdown,
                             transitioner_state = {shutdown, Ref} }) ->
    {stop, normal, set_status(shutdown, State)};
handle_info({shutdown, _Ref}, State) ->
    %% Something saved us in the meanwhilst!
    {noreply, State};
handle_info({transitioner_delay, Event},
            State = #state { status             = {transitioner, TModule},
                             transitioner_state = TState }) ->
    {noreply,
     process_transitioner_response(TModule:event(Event, TState), State)};
handle_info({transitioner_delay, _Event}, State) ->
    {noreply, State};
handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------
%% Cluster config loading and selection
%%----------------------------------------------------------------------------

begin_clustering(State) ->
    {NewConfig, OldConfig} =
        case choose_config() of
            {undefined, undefined} ->
                %% No config at all, 'join' the default.
                {rabbit_clusterer_utils:default_config(), undefined};
            {NewConfig1, undefined} ->
                {NewConfig1, undefined};
            {undefined, OldConfig1} ->
                {OldConfig1, OldConfig1};
            {NewConfig1, OldConfig1} ->
                case rabbit_clusterer_utils:compare_configs(NewConfig1, OldConfig1) of
                    gt ->
                        %% New cluster config has been applied
                        NewConfig2 =
                            rabbit_clusterer_utils:merge_configs(NewConfig1, OldConfig1),
                        {NewConfig2, OldConfig1};
                    _ ->
                        %% All other cases, we ignore the user-provided config.
                        %% TODO: we might want to log this decision.
                        {OldConfig1, OldConfig1}
                end
        end,
    begin_transition(NewConfig, State #state { config = OldConfig }).

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
%% Status changes state machine
%%----------------------------------------------------------------------------

%% Here we enforce the state machine of valid changes to status
%% preboot           -> a transitioner module ({transitioner, TModule})
%% {transitioner, _} -> a transitioner module
%% {transitioner, _} -> pending_shutdown
%% {transitioner, _} -> booting
%% pending_shutdown  -> shutdown
%% pending_shutdown  -> a transitioner module
%% booting           -> ready
%% ready             -> a transitioner module

set_status(NewStatus, State = #state { status = {transitioner, _} })
  when ?IS_TRANSITIONER(NewStatus) ->
    State #state { status = NewStatus };
set_status(NewStatus, State = #state { status = OldStatus })
  when (OldStatus =:= preboot orelse
        OldStatus =:= pending_shutdown orelse
        OldStatus =:= ready) andalso
       ?IS_TRANSITIONER(NewStatus) ->
    ok = rabbit_clusterer_utils:ensure_start_mnesia(),
    State #state { status = {transitioner, NewStatus} };
set_status(pending_shutdown, State = #state { status = {transitioner, _} }) ->
    ok = rabbit_clusterer_utils:stop_mnesia(),
    State #state { status = pending_shutdown };
set_status(booting, State = #state { status       = {transitioner, _},
                                     boot_blocker = Blocker }) ->
    case Blocker of
        undefined -> ok;
        _         -> gen_server:reply(Blocker, ok)
    end,
    State #state { status = booting, boot_blocker = undefined };
set_status(ready, State = #state { status = booting }) ->
    State #state { status = ready };
set_status(shutdown, State = #state { status       = pending_shutdown,
                                      boot_blocker = Blocker }) ->
    case Blocker of
        undefined -> ok;
        _         -> gen_server:reply(Blocker, shutdown)
    end,
    State #state { status = shutdown, boot_blocker = undefined }.


%%----------------------------------------------------------------------------
%% Changing cluster config
%%----------------------------------------------------------------------------

select_transition_module(NewConfig, OldConfig) ->
    case rabbit_clusterer_utils:detect_melisma(NewConfig, OldConfig) of
        true  -> rabbit_clusterer_rejoin;
        false -> rabbit_clusterer_join
    end.

begin_transition(NewConfig, State = #state { config = OldConfig }) ->
    TModule = select_transition_module(NewConfig, OldConfig),
    {ok, Comms, State1} = fresh_comms(State),
    process_transitioner_response(
      TModule:init(NewConfig, Comms), set_status(TModule, State1)).

process_transitioner_response({continue, TState}, State) ->
    State #state { transitioner_state = TState };
process_transitioner_response({continue, Reply, TState}, State) ->
    {Reply, State #state { transitioner_state = TState }};
process_transitioner_response({shutdown, Config}, State) ->
    %% If we've had a config applied to us that tells us to shutdown,
    %% we must record that config, otherwise we can later be restarted
    %% and try to start up with an outdated config.
    ok = write_internal_config(Config),
    {ok, State1} = stop_comms(State #state { config = Config }),
    reset_shutdown(set_status(pending_shutdown, State1));
process_transitioner_response({success, ConfigNew},
                              State = #state { config = ConfigOld }) ->
    %% Just to be safe, merge through the configs
    ConfigNew1 = rabbit_clusterer_utils:merge_configs(ConfigNew, ConfigOld),
    ok = write_internal_config(ConfigNew1),
    {ok, State1} = stop_comms(State #state { transitioner_state = undefined,
                                             config             = ConfigNew1 }),
    set_status(booting, State1);
process_transitioner_response({config_changed, ConfigNew},
                              State = #state { config = ConfigOld }) ->
    %% Just to be safe, merge through the configs
    ConfigNew1 = rabbit_clusterer_utils:merge_configs(ConfigNew, ConfigOld),
    %% If the config has changed then we must now be joining a new
    %% config
    begin_transition(ConfigNew1, State);
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

reset_shutdown(State = #state {
                          status = pending_shutdown,
                          config = #config { shutdown_timeout = infinity } }) ->
    State #state { transitioner_state = undefined };
reset_shutdown(State = #state {
                          status = pending_shutdown,
                          config = #config { shutdown_timeout = Timeout } }) ->
    Ref = make_ref(),
    erlang:send_after(Timeout*1000, self(), {shutdown, Ref}),
    State #state { transitioner_state = {shutdown, Ref} };
reset_shutdown(State) ->
    State.
