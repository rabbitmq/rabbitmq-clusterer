-module(rabbit_clusterer_coordinator).

-behaviour(gen_server).

-export([await_coordination/0,
         ready_to_cluster/0,
         send_new_config/2,
         template_new_config/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(JOIN  , rabbit_clusterer_join).
-define(REJOIN, rabbit_clusterer_rejoin).
-define(IS_TRANSITIONER_MODULE(X), (X =:= ?JOIN orelse X =:= ?REJOIN)).
-define(IS_TRANSITIONER(X),
        (X =:= {transitioner, ?JOIN} orelse X =:= {transitioner, ?REJOIN})).

-record(state, { node_id,
                 status,
                 boot_blocker,
                 config,
                 transitioner_state,
                 comms,
                 nodes,
                 alive_mrefs,
                 dead,
                 poke_timer_ref
               }).

-include("rabbit_clusterer.hrl").

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

await_coordination() ->
    gen_server:call(?SERVER, await_coordination, infinity).

ready_to_cluster() ->
    gen_server:cast(?SERVER, ready_to_cluster),
    ok.

send_new_config(Config, Node) when is_atom(Node) ->
    gen_server:cast({?SERVER, Node}, template_new_config(Config)),
    ok;
send_new_config(_Config, []) ->
    ok;
send_new_config(Config, Nodes) when is_list(Nodes) ->
    abcast = gen_server:abcast(
               lists:usort(Nodes), ?SERVER, template_new_config(Config)),
    ok.

template_new_config(Config) ->
    {new_config, Config, node()}.

%%----------------------------------------------------------------------------

init([]) ->
    ok = rabbit_mnesia:ensure_mnesia_dir(),
    {ok, #state { node_id            = undefined,
                  status             = preboot,
                  boot_blocker       = undefined,
                  config             = undefined,
                  transitioner_state = undefined,
                  comms              = undefined,
                  nodes              = [],
                  alive_mrefs        = [],
                  dead               = [],
                  poke_timer_ref     = undefined
                }}.


%%----------------
%% Call
%%----------------
handle_call(await_coordination, From,
            State = #state { node_id      = NodeID,
                             status       = preboot,
                             config       = Config,
                             boot_blocker = undefined }) ->
    %% We deliberately don't start doing any clustering work until we
    %% get this call. This is because we need to wait for the boot
    %% step to make the call as only by then can we be sure that
    %% things like the file_handle_cache have been started up.
    ExternalConfig = load_external_config(),
    InternalConfig = case Config of
                         undefined -> load_internal_config();
                         _         -> {NodeID, Config}
                     end,
    {NewNodeID, NewConfig, OldConfig} =
        case {ExternalConfig, InternalConfig} of
            {undefined, undefined} ->
                %% No config at all, 'join' the default.
                {NodeID1, NewConfig1} = rabbit_clusterer_utils:default_config(),
                {NodeID1, NewConfig1, undefined};
            {{NodeID1, NewConfig1}, undefined} ->
                {NodeID1, NewConfig1, undefined};
            {undefined, {NodeID1, OldConfig1}} ->
                {NodeID1, OldConfig1, OldConfig1};
            {{NodeID1, NewConfig1}, {NodeID1, OldConfig1}} ->
                case rabbit_clusterer_utils:compare_configs(NewConfig1,
                                                            OldConfig1) of
                    gt ->
                        %% New cluster config has been applied
                        {NodeID1, NewConfig1, OldConfig1};
                    _ ->
                        %% All other cases, we ignore the
                        %% user-provided config.
                        %% TODO: we might want to log this decision.
                        {NodeID1, OldConfig1, OldConfig1}
                end
        end,
    {noreply,
     begin_transition(NewConfig, State #state { node_id      = NewNodeID,
                                                config       = OldConfig,
                                                boot_blocker = From })};

%% request_status is only called by transitioners.
handle_call({request_status, _Node, _NodeID}, _From,
            State = #state { status = preboot }) ->
    %% If status = preboot then we have the situation that a remote
    %% node is contacting us (it's in {transitioner,_}) before we've
    %% even started reading in our cluster configs. We need to ignore
    %% them. They'll either wait for us, or they'll start up and bring
    %% us in later on anyway.
    {reply, preboot, State};
handle_call({request_status, NewNode, NewNodeID}, From,
            State = #state { transitioner_state = TState,
                             status    = Status = {transitioner, TModule} }) ->
    Fun = fun (Config) -> gen_server:reply(From, {Config, Status}), ok end,
    {noreply,
     process_transitioner_response(
       TModule:event({request_config, NewNode, NewNodeID, Fun}, TState), State)
    };
handle_call({request_status, NewNode, NewNodeID}, _From,
            State = #state { node_id = NodeID,
                             config  = Config,
                             status  = Status }) ->
    %% Status \in {pending_shutdown, booting, ready}
    {_NodeIDChanged, Config1} =
        rabbit_clusterer_utils:add_node_id(NewNode, NewNodeID, NodeID, Config),
    State1 = reschedule_shutdown(State #state { config = Config1 }),
    {reply, {Config1, Status}, State1};

%% anything else kills us
handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.


%%----------------
%% Cast
%%----------------
handle_cast({comms, Comms, Result},
            State = #state { comms              = Comms,
                             status             = {transitioner, TModule},
                             transitioner_state = TState }) ->
    %% This is a response from the comms process coming back to the
    %% transitioner
    {noreply, process_transitioner_response(
                TModule:event({comms, Result}, TState), State)};
handle_cast({comms, _Comms, _Result}, State) ->
    %% Ignore it - either we're not transitioning, or it's from an old
    %% comms pid.
    {noreply, State};

handle_cast({new_config, _ConfigRemote, Node},
            State = #state { status = Status, nodes  = Nodes })
  when Status =:= preboot orelse Status =:= booting ->
    %% In preboot we don't know what our eventual config is going to
    %% be so as a result we just ignore the provided remote config but
    %% make a note to send over our eventual config to this node once
    %% we've sorted ourselves out. In booting, it's not safe to
    %% reconfigure our own rabbit, and given the transitioning state
    %% of mnesia during rabbit boot we don't want anyone else to
    %% interfere either, so again, we just wait.
    %%
    %% Don't worry about dupes, we'll filter them out when we come to
    %% deal with the list.
    {noreply, State #state { nodes = [Node | Nodes] }};
handle_cast({new_config, ConfigRemote, Node},
            State = #state { status = {transitioner, TModule},
                             transitioner_state = TState }) ->
    %% We have to deal with this case because we could have the
    %% situation where we are blocked in the transitioner waiting for
    %% another node to come up but there really is a younger config
    %% that has become available that we should be transitioning
    %% to. If we don't deal with this we can potentially have a
    %% deadlock.
    {noreply,
     process_transitioner_response(
       TModule:event({new_config, ConfigRemote, Node}, TState), State)};
handle_cast({new_config, ConfigRemote, Node},
            State = #state { config = Config }) ->
    %% Status is either running on pending_shutdown. In both cases, we
    %% a) know what our config really is; b) it's safe to begin
    %% transitions to other configurations.
    case rabbit_clusterer_utils:compare_configs(ConfigRemote, Config) of
        lt ->
            ok = send_new_config(Config, Node),
            {noreply, State};
        gt ->
            %% Remote is younger. We should switch to it. We
            %% deliberately do not merge across the configs at this
            %% stage as it would break melisma detection.
            %% begin_transition will reboot if necessary.
            {noreply, begin_transition(ConfigRemote, State)};
        _ ->
            %% eq and invalid. We might want eventually to do
            %% something more active on invalid. TODO
            {noreply, State}
    end;

handle_cast(ready_to_cluster, State = #state { status = booting }) ->
    %% Note that we don't allow any transition to start whilst we're
    %% booting so it should be safe to assert we can only receive
    %% ready_to_cluster when in booting.
    {noreply, set_status(ready, State)};

%% anything else kills us
handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.


%%----------------
%% Info
%%----------------
handle_info({shutdown, Ref},
            State = #state { status = pending_shutdown,
                             transitioner_state = {shutdown, Ref} }) ->
    %% This is the timer coming back to us. We actually need to halt
    %% the VM here (which set_status does). Sadly, if there is a boot
    %% blocker, it'll see our process death (unavoidable: if the
    %% supervision tree tears us down before rabbit then this'll
    %% happen regardless) and spew an error to the console.
    {stop, normal, set_status(shutdown, State)};
handle_info({shutdown, _Ref}, State) ->
    %% Something saved us in the meanwhilst!
    {noreply, State};

handle_info({transitioner_delay, Event},
            State = #state { status             = {transitioner, TModule},
                             transitioner_state = TState }) ->
    %% A transitioner wanted some sort of timer based callback. Note
    %% it is the transitioner's responsibility to filter out
    %% invalid/outdated etc delayed events.
    {noreply,
     process_transitioner_response(TModule:event(Event, TState), State)};
handle_info({transitioner_delay, _Event}, State) ->
    {noreply, State};

%% Monitoring stuff
handle_info({'DOWN', MRef, process, {?SERVER, Node}, _Info},
            State = #state { alive_mrefs = Alive, dead = Dead }) ->
    case lists:delete(MRef, Alive) of
        Alive  -> {noreply, State};
        Alive1 -> {noreply, ensure_poke_timer(
                              State #state { alive_mrefs = Alive1,
                                             dead        = [Node | Dead] })}
    end;
handle_info(poke_the_dead, State = #state { dead        = Dead,
                                            alive_mrefs = Alive,
                                            status      = ready,
                                            config      = Config }) ->
    %% When we're transitioning to something else (or even booting) we
    %% don't bother with the poke as the transitioner will take care
    %% of updating nodes we want to cluster with, and when the boot
    %% finishes we'll update everyone in the old config and people who
    %% have sent us new_config msgs too.
    MRefsNew = [monitor(process, {?SERVER, N}) || N <- Dead],
    ok = send_new_config(Config, Dead),
    Alive1 = MRefsNew ++ Alive,
    {noreply, State #state { dead = [], alive_mrefs = Alive1,
                             poke_timer_ref = undefined }};
handle_info(poke_the_dead, State) ->
    {noreply, State #state { poke_timer_ref = undefined }};

%% anything else kills us
handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------
%% Status changes state machine
%%----------------------------------------------------------------------------

%% Here we enforce the state machine of valid changes to status. This
%% is also the only place we inspect the boot blocker.

%% preboot           -> a transitioner module ({transitioner, TModule})
%% preboot           -> pending_shutdown
%% {transitioner, _} -> a transitioner module
%% {transitioner, _} -> pending_shutdown
%% {transitioner, _} -> booting
%% pending_shutdown  -> shutdown
%% pending_shutdown  -> pending_shutdown
%% pending_shutdown  -> preboot
%% booting           -> ready
%% booting           -> pending_shutdown
%% ready             -> pending_shutdown


set_status(NewStatus, State = #state { status = {transitioner, _} })
  when ?IS_TRANSITIONER_MODULE(NewStatus) ->
    ok = rabbit_clusterer_utils:ensure_start_mnesia(),
    State #state { status = {transitioner, NewStatus} };
set_status(NewStatus, State = #state { status = OldStatus })
  when OldStatus =:= preboot andalso
       ?IS_TRANSITIONER_MODULE(NewStatus) ->
    ok = rabbit_clusterer_utils:ensure_start_mnesia(),
    State #state { status = {transitioner, NewStatus} };
set_status(pending_shutdown, State = #state { boot_blocker = Blocker }) ->
    case Blocker of
        undefined ->
            %% No boot blocker, we should stop the rabbit application
            %% (and mnesia too). Problem though is that rabbit may not
            %% have fully booted up, so the stop_rabbit may fail. We
            %% ignore this problem here, but if we ever go on into
            %% preboot, the process we spawn to start up rabbit
            %% repeatedly tries to stop it first.
            ok = case rabbit_clusterer_utils:stop_rabbit() of
                     ok -> rabbit_clusterer_utils:stop_mnesia();
                     _  -> ok
                 end;
        _ ->
            %% We have a blocker, so we're just going to hold the
            %% rabbit boot right here.
            ok
    end,
    State #state { status = pending_shutdown };
set_status(booting, State = #state { status       = {transitioner, _},
                                     boot_blocker = Blocker }) ->
    case Blocker of
        undefined -> ok;
        _         -> gen_server:reply(Blocker, ok)
    end,
    State #state { status = booting, boot_blocker = undefined };
set_status(ready, State = #state { status = booting }) ->
    update_monitoring(State #state { status = ready });
set_status(shutdown, State = #state { status = pending_shutdown }) ->
    %% Regardless of the blocker, we just do the init:stop() as it's
    %% by far the surest way to stop the VM.
    init:stop(),
    State #state { status = shutdown, boot_blocker = undefined };
set_status(preboot, State = #state { status       = pending_shutdown,
                                     boot_blocker = undefined }) ->
    spawn(fun restart_rabbit/0),
    State #state { status = preboot };
set_status(preboot, State = #state { status = pending_shutdown }) ->
    State #state { status = preboot }.


restart_rabbit() ->
    case rabbit_clusterer_utils:stop_rabbit() of
        ok ->
            ok = rabbit_clusterer_utils:stop_mnesia(),
            ok = rabbit_clusterer_utils:ensure_start_mnesia(),
            ok = application:ensure_started(rabbit);
        _ ->
            timer:sleep(500),
            restart_rabbit()
    end.


%%----------------------------------------------------------------------------
%% Cluster config loading and selection
%%----------------------------------------------------------------------------

%% We can't put the file within mnesia dir because that upsets the
%% virgin detection in rabbit_mnesia!
internal_config_path() -> rabbit_mnesia:dir() ++ "-cluster.config".

external_config_path() -> application:get_env(rabbit, cluster_config).

load_external_config() ->
    ExternalProplist =
        case external_config_path() of
            {ok, ExternalProplist1 = [{_,_}|_]} ->
                OldPath = proplists:get_value(old_path, ExternalProplist1),
                ok = application:set_env(rabbit, cluster_config, OldPath),
                proplists:delete(old_path, ExternalProplist1);
            {ok, ExternalPath = [_|_]} ->
                case rabbit_file:read_term_file(ExternalPath) of
                    {error, enoent}           -> undefined;
                    {ok, [ExternalProplist1]} -> ExternalProplist1
                end;
            undefined ->
                undefined
        end,
    case ExternalProplist of
        undefined -> undefined;
        _         -> rabbit_clusterer_utils:proplist_config_to_record(
                       ExternalProplist)
    end.

load_internal_config() ->
    InternalProplist =
        case rabbit_file:read_term_file(internal_config_path()) of
            {error, enoent}                       -> undefined;
            {ok, [InternalProplist1 = [{_,_}|_]]} -> InternalProplist1
        end,
    case InternalProplist of
        undefined -> undefined;
        _         -> rabbit_clusterer_utils:proplist_config_to_record(
                       InternalProplist)
    end.

write_internal_config(NodeID, Config) ->
    Proplist = rabbit_clusterer_utils:record_config_to_proplist(NodeID, Config),
    ok = rabbit_file:write_term_file(internal_config_path(), [Proplist]).

maybe_reboot_into_config(Config,
                         State = #state { status       = pending_shutdown,
                                          boot_blocker = undefined,
                                          node_id      = NodeID }) ->
    %% Right, so with no boot blocker we need to set up the config in
    %% the env, start up the rabbit application and await the boot
    %% step hook.
    Proplist =
        [{old_path, external_config_path()}
         | rabbit_clusterer_utils:record_config_to_proplist(NodeID, Config)],
    ok = application:set_env(rabbit, cluster_config, Proplist),
    set_status(preboot, State);
maybe_reboot_into_config(Config,
                         State = #state { status = pending_shutdown }) ->
    begin_transition(Config, set_status(preboot, State)).


%%----------------------------------------------------------------------------
%% Changing cluster config
%%----------------------------------------------------------------------------

begin_transition(NewConfig, State = #state { node_id = NodeID,
                                             config  = OldConfig,
                                             nodes   = Nodes,
                                             status  = Status }) ->
    true = Status =/= booting, %% ASSERTION
    case rabbit_clusterer_utils:node_in_config(NewConfig) of
        false ->
            process_transitioner_response({shutdown, NewConfig}, State);
        true ->
            case Status of
                pending_shutdown ->
                    %% Don't merge configs here otherwise it can break
                    %% melisma detection.
                    maybe_reboot_into_config(NewConfig, State);
                _ ->
                    Melisma = rabbit_clusterer_utils:detect_melisma(NewConfig,
                                                                    OldConfig),
                    Action =
                        case {Status, Melisma} of
                            {ready, true } -> noop;
                            {ready, false} -> reboot;
                            {_    , true } -> {transitioner, ?REJOIN};
                            {_    , false} -> {transitioner, ?JOIN}
                        end,
                    NewConfig1 = rabbit_clusterer_utils:merge_configs(
                                   NodeID, NewConfig, OldConfig),
                    case Action of
                        noop ->
                            update_monitoring(
                              State #state { config = NewConfig1 });
                        reboot ->
                            %% Must use the un-merged config here
                            %% otherwise we risk getting a different
                            %% answer from detect_melisma when we come
                            %% back here after reboot.
                            maybe_reboot_into_config(
                              NewConfig, set_status(pending_shutdown, State));
                        {transitioner, TModule} ->
                            ok = send_new_config(NewConfig1, Nodes),
                            %% Wipe out alive_mrefs and dead so that
                            %% if we get DOWN's we don't care about
                            %% them.
                            {Comms, State1} =
                                fresh_comms(State #state { alive_mrefs = [],
                                                           dead        = [],
                                                           nodes       = [] }),
                            process_transitioner_response(
                              TModule:init(NewConfig1, NodeID, Comms),
                              set_status(TModule, State1))
                    end
            end
    end.

process_transitioner_response({continue, TState}, State) ->
    State #state { transitioner_state = TState };
process_transitioner_response({SuccessOrShutdown, ConfigNew},
                              State = #state { node_id = NodeID,
                                               config  = ConfigOld })
  when SuccessOrShutdown =:= success orelse SuccessOrShutdown =:= shutdown ->
    %% Both success and shutdown are treated the same as they're exit
    %% nodes from the states of the transitioners. If we've had a
    %% config applied to us that tells us to shutdown, we must record
    %% that config, otherwise we can later be restarted and try to
    %% start up with an outdated config.
    ok = write_internal_config(NodeID, ConfigNew),
    State1 = stop_comms(State #state { transitioner_state = undefined,
                                       config             = ConfigNew }),
    case SuccessOrShutdown of
        success  -> %% Wait for the ready transition before updating monitors
                    set_status(booting, State1);
        shutdown -> schedule_shutdown(
                      set_status(pending_shutdown, update_monitoring(State1)))
    end;
process_transitioner_response({config_changed, ConfigNew}, State) ->
    %% begin_transition relies on unmerged configs, so don't merge
    %% through here.
    begin_transition(ConfigNew, State);
process_transitioner_response({sleep, Delay, Event, TState}, State) ->
    erlang:send_after(Delay, self(), {transitioner_delay, Event}),
    State #state { transitioner_state = TState }.


fresh_comms(State) ->
    State1 = stop_comms(State),
    {ok, Token} = rabbit_clusterer_comms_sup:start_comms(),
    {Token, State1 #state { comms = Token }}.

stop_comms(State = #state { comms = undefined }) ->
    State;
stop_comms(State = #state { comms = Token }) ->
    ok = rabbit_clusterer_comms:stop(Token),
    State #state { comms = undefined }.

schedule_shutdown(State = #state {
                          status = pending_shutdown,
                          config = #config { shutdown_timeout = infinity } }) ->
    State #state { transitioner_state = undefined };
schedule_shutdown(State = #state {
                          status = pending_shutdown,
                          config = #config { shutdown_timeout = Timeout } }) ->
    Ref = make_ref(),
    erlang:send_after(Timeout*1000, self(), {shutdown, Ref}),
    State #state { transitioner_state = {shutdown, Ref} };
schedule_shutdown(State) ->
    State.

reschedule_shutdown(
  State = #state { status = pending_shutdown,
                   transitioner_state = {shutdown, _Ref},
                   config = #config { shutdown_timeout = Timeout } }) ->
    true = Timeout =/= infinity, %% ASSERTION
    schedule_shutdown(State);
reschedule_shutdown(State) ->
    State.

update_monitoring(
  State = #state { config      = ConfigNew = #config { nodes = NodesNew },
                   nodes       = NodesOld,
                   alive_mrefs = AliveOld }) ->
    MyNode = node(),
    ok = send_new_config(ConfigNew, NodesOld),
    [demonitor(MRef) || MRef <- AliveOld],
    NodeNamesNew = [N || {N, _} <- NodesNew, N =/= MyNode],
    AliveNew = [monitor(process, {?SERVER, N}) || N <- NodeNamesNew],
    State #state { nodes          = NodeNamesNew,
                   alive_mrefs    = AliveNew,
                   dead           = [],
                   poke_timer_ref = undefined }.

ensure_poke_timer(State = #state { poke_timer_ref = undefined }) ->
    %% TODO: justify 2000
    State #state { poke_timer_ref =
                       erlang:send_after(2000, self(), poke_the_dead) };
ensure_poke_timer(State) ->
    State.
