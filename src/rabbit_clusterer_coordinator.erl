-module(rabbit_clusterer_coordinator).

-behaviour(gen_server).

-export([begin_coordination/0,
         rabbit_booted/0,
         send_new_config/2,
         template_new_config/1,
         apply_config/1]).

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
                 config,
                 transitioner_state,
                 comms,
                 nodes,
                 alive_mrefs,
                 dead,
                 poke_timer_ref,
                 booted
               }).

-include("rabbit_clusterer.hrl").

start_link() -> gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

begin_coordination() -> ok = gen_server:cast(?SERVER, begin_coordination).

rabbit_booted() -> ok = gen_server:cast(?SERVER, rabbit_booted).

send_new_config(Config, Node) when is_atom(Node) ->
    %% Node may be undefined. gen_server:cast doesn't error. This is
    %% what we want.
    ok = gen_server:cast({?SERVER, Node}, template_new_config(Config));
send_new_config(_Config, []) ->
    ok;
send_new_config(Config, Nodes) when is_list(Nodes) ->
    abcast = gen_server:abcast(
               lists:usort(Nodes), ?SERVER, template_new_config(Config)),
    ok.

template_new_config(Config) -> {new_config, Config, node()}.

apply_config(Config) ->
    gen_server:call(?SERVER, {apply_config, Config}, infinity).

%%----------------------------------------------------------------------------

init([]) -> {ok, #state { node_id            = undefined,
                          status             = preboot,
                          config             = undefined,
                          transitioner_state = undefined,
                          comms              = undefined,
                          nodes              = [],
                          alive_mrefs        = [],
                          dead               = [],
                          poke_timer_ref     = undefined,
                          booted             = false
                        }}.


%%----------------
%% Call
%%----------------
%% request_status is only called by transitioners.
handle_call({request_status, _Node, _NodeID}, _From,
            State = #state { status = preboot }) ->
    %% If status = preboot then we have the situation that a remote
    %% node is contacting us (it's in {transitioner,_}) before we've
    %% even started reading in our cluster configs. We need to "ignore"
    %% them. They'll either wait for us, or they'll start up and bring
    %% us in later on anyway.
    {reply, preboot, State};
handle_call({request_status, NewNode, NewNodeID}, From,
            State = #state { status = Status = {transitioner, _} }) ->
    Fun = fun (Config) -> gen_server:reply(From, {Config, Status}), ok end,
    {noreply, transitioner_event(
                {request_config, NewNode, NewNodeID, Fun}, State)};
handle_call({request_status, _NewNode, _NewNodeID}, _From,
            State = #state { config = Config, status = Status }) ->
    %% Status \in {pending_shutdown, booting, ready}
    {reply, {Config, Status}, reschedule_shutdown(State)};

%% This is where a call from TModule on one node to TModule on another
%% node lands.
handle_call({{transitioner, _TModule} = Status, Msg}, From,
            State = #state { status = Status }) ->
    Fun = fun (Result) -> gen_server:reply(From, Result), ok end,
    {noreply, transitioner_event({Msg, Fun}, State)};
handle_call({{transitioner, _TModule}, _Msg}, _From, State) ->
    {reply, invalid, State};

handle_call({apply_config, NewConfig}, From,
            State = #state { status = Status, config = Config })
  when Status =:= ready orelse Status =:= pending_shutdown
       orelse ?IS_TRANSITIONER(Status) ->
    case {rabbit_clusterer_config:load(NewConfig), Status} of
        {#config {} = NewConfig1, {transitioner, _}} ->
            %% We have to defer to the transitioner here which means
            %% we can't give back as good feedback, but never
            %% mind. The transitioner will do the comparison for us
            %% with whatever it's currently trying to transition to.
            gen_server:reply(From, transition_in_progress_ok),
            {noreply, transitioner_event(
                        {new_config, NewConfig1, undefined}, State)};
        {#config {} = NewConfig1, _} ->
            case rabbit_clusterer_config:compare(NewConfig1, Config) of
                lt -> {reply, {provided_config_is_older_than_current,
                               NewConfig1, Config}, State};
                eq -> {reply, {provided_config_already_applied,
                               NewConfig1}, State};
                gt -> gen_server:reply(
                        From,
                        {beginning_transition_to_provided_config, NewConfig1}),
                      {noreply, begin_transition(NewConfig1, State)};
                invalid ->
                    {reply,
                     {provided_config_has_same_version_but_differs_from_current,
                      NewConfig1, Config}, State}
            end;
        {{error, Reason}, _} ->
            {reply, {invalid_config_specification, NewConfig, Reason}, State}
    end;
handle_call({apply_config, _Config}, _From,
            State = #state { status = Status }) ->
    {reply, {cannot_apply_config_currently, Status}, State};

%% anything else kills us
handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.


%%----------------
%% Cast
%%----------------
handle_cast(begin_coordination, State = #state { node_id = NodeID,
                                                 status  = preboot,
                                                 config  = Config }) ->
    {NewNodeID, NewConfig, OldConfig} = rabbit_clusterer_config:load(
                                          NodeID, Config),
    {noreply,
     begin_transition(NewConfig, State #state { node_id = NewNodeID,
                                                config  = OldConfig })};
handle_cast(begin_coordination, State) ->
    {noreply, State};

handle_cast({comms, Comms, Result},
            State = #state { comms = Comms, status = {transitioner, _} }) ->
    %% This is a response from the comms process coming back to the
    %% transitioner
    {noreply, transitioner_event({comms, Result}, State)};
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
            State = #state { status = {transitioner, _} }) ->
    %% We have to deal with this case because we could have the
    %% situation where we are blocked in the transitioner waiting for
    %% another node to come up but there really is a younger config
    %% that has become available that we should be transitioning
    %% to. If we don't deal with this we can potentially have a
    %% deadlock.
    {noreply, transitioner_event({new_config, ConfigRemote, Node}, State)};
handle_cast({new_config, ConfigRemote, Node},
            State = #state { config = Config }) ->
    %% Status is either running on pending_shutdown. In both cases, we
    %% a) know what our config really is; b) it's safe to begin
    %% transitions to other configurations.
    case rabbit_clusterer_config:compare(ConfigRemote, Config) of
        lt -> ok = send_new_config(Config, Node),
              {noreply, State};
        gt -> %% Remote is younger. We should switch to it. We
              %% deliberately do not merge across the configs at this
              %% stage as it would break melisma detection.
              %% begin_transition will reboot if necessary.
              {noreply, begin_transition(ConfigRemote, State)};
        _  -> %% eq and invalid. In both cases we just ignore. If
              %% invalid, the fact is that we are stable - either
              %% running or pending_shutdown, so we don't want to
              %% disturb that.
              {noreply, State}
    end;

handle_cast(rabbit_booted, State = #state { status = booting }) ->
    %% Note that we don't allow any transition to start whilst we're
    %% booting so it should be safe to assert we can only receive
    %% ready_to_cluster when in booting.
    {noreply, set_status(ready, State)};

handle_cast({lock, Locker}, State = #state { comms = undefined }) ->
    gen_server:cast(Locker, {lock_rejected, node()}),
    {noreply, State};
handle_cast({lock, Locker}, State = #state { comms = Comms }) ->
    ok = rabbit_clusterer_comms:lock(Locker, Comms),
    {noreply, State};
handle_cast({unlock, _Locker}, State = #state { comms = undefined }) ->
    {noreply, State};
handle_cast({unlock, Locker}, State = #state { comms = Comms }) ->
    ok = rabbit_clusterer_comms:unlock(Locker, Comms),
    {noreply, State};

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
    %% the VM here (which set_status does).
    {stop, normal, set_status(shutdown, State)};
handle_info({shutdown, _Ref}, State) ->
    %% Something saved us in the meanwhilst!
    {noreply, State};

handle_info({transitioner_delay, Event},
            State = #state { status = {transitioner, _} }) ->
    %% A transitioner wanted some sort of timer based callback. Note
    %% it is the transitioner's responsibility to filter out
    %% invalid/outdated etc delayed events.
    {noreply, transitioner_event(Event, State)};
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
    %% of updating nodes we want to cluster with and the surrounding
    %% code will update the nodes we're currently clustered with and
    %% any other nodes that contacted us whilst we were transitioning
    %% or booting.
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


terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.


%%----------------------------------------------------------------------------
%% Status changes state machine
%%----------------------------------------------------------------------------

%% Here we enforce the state machine of valid changes to status.

%% preboot           -> a transitioner module ({transitioner, TModule})
%% preboot           -> pending_shutdown
%% {transitioner, _} -> a transitioner module
%% {transitioner, _} -> pending_shutdown
%% {transitioner, _} -> booting
%% pending_shutdown  -> shutdown
%% pending_shutdown  -> pending_shutdown
%% pending_shutdown  -> a transitioner module
%% booting           -> ready
%% booting           -> pending_shutdown
%% ready             -> pending_shutdown


set_status(NewStatus, State = #state { status = {transitioner, _} })
  when ?IS_TRANSITIONER_MODULE(NewStatus) ->
    State #state { status = {transitioner, NewStatus} };
set_status(NewStatus, State = #state { status = OldStatus })
  when (OldStatus =:= preboot orelse OldStatus =:= pending_shutdown)
       andalso ?IS_TRANSITIONER_MODULE(NewStatus) ->
    State #state { status = {transitioner, NewStatus} };
set_status(pending_shutdown, State = #state { status = ready }) ->
    %% Even though we think we're ready, there might still be some
    %% rabbit boot actions going on...
    error_logger:info_msg("Clusterer stopping Rabbit pending shutdown.~n"),
    ok = rabbit:await_startup(),
    ok = rabbit_clusterer_utils:stop_rabbit(),
    ok = rabbit_clusterer_utils:stop_mnesia(),
    State #state { status = pending_shutdown };
set_status(pending_shutdown, State = #state { status = Status }) ->
    true = Status =/= booting, %% ASSERTION
    State #state { status = pending_shutdown };
set_status(booting, State = #state { status  = {transitioner, _},
                                     config  = Config,
                                     booted  = Booted,
                                     node_id = NodeID }) ->
    error_logger:info_msg(
      "Clusterer booting Rabbit into cluster configuration:~n~p~n",
      [rabbit_clusterer_config:to_proplist(NodeID, Config)]),
    ok = rabbit_clusterer_utils:ensure_start_mnesia(),
    case Booted of
        true  -> ok = rabbit_clusterer_utils:start_rabbit_async();
        false -> ok = rabbit_clusterer_utils:boot_rabbit_async()
    end,
    State #state { status = booting, booted = true };
set_status(ready, State = #state { status = booting }) ->
    error_logger:info_msg("Cluster achieved and Rabbit running.~n"),
    update_monitoring(State #state { status = ready });
set_status(shutdown, State = #state { status = pending_shutdown }) ->
    error_logger:info_msg("Clusterer stopping node now.~n"),
    init:stop(),
    State #state { status = shutdown }.

%%----------------------------------------------------------------------------
%% Changing cluster config
%%----------------------------------------------------------------------------

begin_transition(NewConfig, State = #state { node_id = NodeID,
                                             config  = OldConfig,
                                             nodes   = Nodes,
                                             status  = Status }) ->
    true = Status =/= booting, %% ASSERTION
    case rabbit_clusterer_config:contains_node(node(), NewConfig) of
        false ->
            process_transitioner_response({shutdown, NewConfig}, State);
        true ->
            Melisma = rabbit_clusterer_config:detect_melisma(NewConfig,
                                                            OldConfig),
            Action = case {Status, Melisma} of
                         {ready, true } -> noop;
                         {ready, false} -> reboot;
                         {_    , true } -> {transitioner, ?REJOIN};
                         {_    , false} -> {transitioner, ?JOIN}
                     end,
            NewConfig1 = rabbit_clusterer_config:merge(
                           NodeID, NewConfig, OldConfig),
            case Action of
                noop ->
                    ok = rabbit_clusterer_config:store_internal(
                           NodeID, NewConfig1),
                    error_logger:info_msg(
                      "Clusterer seemlessly transitioned to new "
                      "configuration:~n~p~n",
                      [rabbit_clusterer_config:to_proplist(
                         NodeID, NewConfig1)]),
                    update_monitoring(
                      State #state { config = NewConfig1 });
                reboot ->
                    %% Must use the un-merged config here otherwise we
                    %% risk getting a different answer from
                    %% detect_melisma when we come back here after
                    %% reboot.
                    begin_transition(
                      NewConfig, set_status(pending_shutdown, State));
                {transitioner, TModule} ->
                    ok = send_new_config(NewConfig1, Nodes),
                    %% Wipe out alive_mrefs and dead so that if we get
                    %% DOWN's we don't care about them.
                    {Comms, State1} =
                        fresh_comms(State #state { alive_mrefs = [],
                                                   dead        = [],
                                                   nodes       = [] }),
                    State2 = set_status(TModule, State1),
                    process_transitioner_response(
                      TModule:init(NewConfig1, NodeID, Comms),
                      State2)
            end
    end.

transitioner_event(Event, State = #state { status = {transitioner, TModule},
                                           transitioner_state = TState }) ->
    process_transitioner_response(TModule:event(Event, TState), State).

process_transitioner_response({continue, TState}, State) ->
    State #state { transitioner_state = TState };
process_transitioner_response({SuccessOrShutdown, ConfigNew},
                              State = #state { node_id = NodeID })
  when SuccessOrShutdown =:= success orelse SuccessOrShutdown =:= shutdown ->
    %% Both success and shutdown are treated the same as they're exit
    %% nodes from the states of the transitioners. If we've had a
    %% config applied to us that tells us to shutdown, we must record
    %% that config, otherwise we can later be restarted and try to
    %% start up with an outdated config.
    ok = rabbit_clusterer_config:store_internal(NodeID, ConfigNew),
    State1 = stop_comms(State #state { transitioner_state = undefined,
                                       config             = ConfigNew }),
    case SuccessOrShutdown of
        success  -> %% Wait for the ready transition before updating monitors
                    set_status(booting, State1);
        shutdown -> schedule_shutdown(
                      update_monitoring(set_status(pending_shutdown, State1)))
    end;
process_transitioner_response({config_changed, ConfigNew}, State) ->
    %% begin_transition relies on unmerged configs, so don't merge
    %% through here.
    begin_transition(ConfigNew, State);
process_transitioner_response({sleep, Delay, Event, TState}, State) ->
    erlang:send_after(Delay, self(), {transitioner_delay, Event}),
    State #state { transitioner_state = TState };
process_transitioner_response({invalid_config, Config},
                              State = #state { node_id = NodeID }) ->
    %% An invalid config was detected somewhere. We shut ourselves
    %% down, but we do not write out the config. Do not
    %% update_monitoring either.
    State1 = stop_comms(State #state { transitioner_state = undefined }),
    error_logger:info_msg("Multiple different configurations with equal "
                          "version numbers detected. Shutting down.~n~p~n",
                          [rabbit_clusterer_config:to_proplist(
                             NodeID, Config)]),
    set_status(shutdown, set_status(pending_shutdown, State1)).


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
                   alive_mrefs = AliveOld,
                   status      = Status })
  when Status =:= ready orelse Status =:= pending_shutdown ->
    ok = send_new_config(ConfigNew, NodesOld),
    [demonitor(MRef) || MRef <- AliveOld],
    {NodeNamesNew, AliveNew} =
        case Status of
            ready ->
                NodeNamesNew1 = [N || {N, _} <- NodesNew, N =/= node()],
                {NodeNamesNew1,
                 [monitor(process, {?SERVER, N}) || N <- NodeNamesNew1]};
            pending_shutdown ->
                %% If we're pending_shutdown it means we're not in
                %% this config. Thus we don't care about who is in
                %% this config, so don't monitor them. They won't be
                %% monitoring us. Even more importantly, don't send
                %% them any new config we get if we get pulled into
                %% some other cluster.
                {[], []}
        end,
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
