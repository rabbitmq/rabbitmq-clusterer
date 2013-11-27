-module(rabbit_clusterer_coordinator).

-behaviour(gen_server).

-export([begin_coordination/0,
         rabbit_booted/0,
         rabbit_boot_failed/0,
         send_new_config/2,
         template_new_config/1,
         apply_config/1,
         request_status/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-define(IS_TRANSITIONER(X), (X =:= {transitioner, join} orelse
                             X =:= {transitioner, rejoin})).

-record(state, { status,
                 node_id,
                 config,
                 transitioner_state,
                 comms,
                 nodes,
                 alive_mrefs,
                 dead,
                 poke_timer_ref,
                 booted
               }).

%%----------------------------------------------------------------------------

start_link() -> gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

begin_coordination() -> ok = gen_server:cast(?SERVER, begin_coordination).

rabbit_booted() -> ok = gen_server:cast(?SERVER, rabbit_booted).

rabbit_boot_failed() -> ok = gen_server:cast(?SERVER, rabbit_boot_failed).

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

request_status(Node) ->
    gen_server:call(
      {?SERVER, Node}, {request_status, undefined, <<>>}, infinity).

%%----------------------------------------------------------------------------

init([]) -> {ok, #state { status             = preboot,
                          node_id            = undefined,
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

%% request_status requires a response and is only used by the
%% transitioners to perform coordination when joining or rejoining a
%% cluster.
handle_call({request_status, _Node, _NodeID}, _From,
            State = #state { status = preboot }) ->
    %% If status = preboot then we have the situation that a remote
    %% node is contacting us (it's in {transitioner,_}) before we've
    %% even started reading in our cluster configs. We need to "ignore"
    %% them. They'll either wait for us, or they'll start up and bring
    %% us in later on anyway.
    reply(preboot, State);
handle_call({request_status, NewNode, NewNodeID}, From,
            State = #state { status = Status = {transitioner, _} }) ->
    Fun = fun (Config) -> gen_server:reply(From, {Config, Status}), ok end,
    noreply(transitioner_event(
              {request_config, NewNode, NewNodeID, Fun}, State));
handle_call({request_status, NewNode, NewNodeID}, _From,
            State = #state { status  = Status,
                             node_id = NodeID,
                             config  = Config }) ->
    %% Status \in {booting, ready}
    %%
    %% Consider we're running (ready) and we're already clustered with
    %% NewNode, though it's currently down and is just coming back up,
    %% after being reset. At this point, we will learn of its new
    %% NodeID, but we must ignore that: if we merged it into our
    %% config here then should NewNode be starting up with a newer
    %% config that eventually involves us, we would lose the ability
    %% in is_compatible to detect the node has been reset. Hence
    %% ignoring NewNodeID here.
    %%
    %% Equally however, consider we're running in a cluster which has
    %% some missing nodes. Those nodes then come online and request
    %% our status. We should here record their ID. So we want to add
    %% their ID in only if we don't have a record of it already.
    %%
    %% This is consistent with the behaviour of the transitioners
    %% (above head) who will restart the transition if the NewNode has
    %% changed its ID.
    Config1 = case rabbit_clusterer_config:add_node_id(NewNode, NewNodeID,
                                                       NodeID, Config) of
                  {true,  _Config} -> Config;
                  {false, Config2} -> Config2
              end,
    reply({Config1, Status}, State #state { config = Config1 });

%% This is where a call from the transitioner on one node to the
%% transitioner on another node lands.
handle_call({{transitioner, _TKind} = Status, Msg}, From,
            State = #state { status = Status }) ->
    Fun = fun (Result) -> gen_server:reply(From, Result), ok end,
    noreply(transitioner_event({Msg, Fun}, State));
handle_call({{transitioner, _TKind}, _Msg}, _From, State) ->
    reply(invalid, State);

handle_call({apply_config, NewConfig}, From,
            State = #state { status = Status,
                             config = Config })
  when Status =:= ready orelse ?IS_TRANSITIONER(Status) ->
    case {rabbit_clusterer_config:load(NewConfig), Status} of
        {{ok, NewConfig1}, {transitioner, _}} ->
            %% We have to defer to the transitioner here which means
            %% we can't give back as good feedback, but never
            %% mind. The transitioner will do the comparison for us
            %% with whatever it's currently trying to transition to.
            gen_server:reply(From, transition_in_progress_ok),
            noreply(transitioner_event(
                      {new_config, NewConfig1, undefined}, State));
        {{ok, NewConfig1}, ready} ->
            ReadyNotRunning = Status =:= ready andalso not rabbit:is_running(),
            case rabbit_clusterer_config:compare(NewConfig1, Config) of
                younger when ReadyNotRunning ->
                           reply({rabbit_not_running, NewConfig1}, State);
                younger -> gen_server:reply(
                             From, {beginning_transition_to_provided_config,
                                    NewConfig1}),
                           noreply(begin_transition(NewConfig1, State));
                older   -> reply({provided_config_is_older_than_current,
                                  NewConfig1, Config}, State);
                coeval  -> reply({provided_config_already_applied,
                                  NewConfig1}, State);
                invalid ->
                    reply(
                      {provided_config_has_same_version_but_differs_from_current,
                       NewConfig1, Config}, State)
            end;
        {{error, Reason}, _} ->
            reply({invalid_config_specification, NewConfig, Reason}, State)
    end;
handle_call({apply_config, _Config}, _From,
            State = #state { status = Status }) ->
    reply({cannot_apply_config_currently, Status}, State);

%% anything else kills us
handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

%%----------------
%% Cast
%%----------------

handle_cast(begin_coordination, State = #state { status  = preboot,
                                                 node_id = NodeID,
                                                 config  = Config }) ->
    {NewNodeID, NewConfig, OldConfig} = rabbit_clusterer_config:load(
                                          NodeID, Config),
    noreply(
      begin_transition(NewConfig, State #state { node_id = NewNodeID,
                                                 config  = OldConfig }));
handle_cast(begin_coordination, State) ->
    noreply(State);

handle_cast({comms, Comms, Result},
            State = #state { comms = Comms, status = {transitioner, _} }) ->
    %% This is a response from the comms process coming back to the
    %% transitioner
    noreply(transitioner_event({comms, Result}, State));
handle_cast({comms, _Comms, _Result}, State) ->
    %% Ignore it - either we're not transitioning, or it's from an old
    %% comms pid.
    noreply(State);

%% new_config is sent to update nodes that we come across through some
%% means that we think they're running an old config and should be
%% updated to run a newer config. It is also sent periodically to any
%% missing nodes in the cluster to make sure that should they appear
%% they will be informed of the cluster config we expect them to take
%% part in.
handle_cast({new_config, _ConfigRemote, Node},
            State = #state { status = preboot,
                             nodes  = Nodes }) ->
    %% In preboot we don't know what our eventual config is going to
    %% be so as a result we just ignore the provided remote config but
    %% make a note to send over our eventual config to this node once
    %% we've sorted ourselves out.
    %%
    %% Don't worry about dupes, we'll filter them out when we come to
    %% deal with the list.
    noreply(State #state { nodes = [Node | Nodes] });
handle_cast({new_config, ConfigRemote, Node},
            State = #state { status  = booting,
                             nodes   = Nodes,
                             node_id = NodeID,
                             config  = Config }) ->
    %% In booting, it's not safe to reconfigure our own rabbit, and
    %% given the transitioning state of mnesia during rabbit boot we
    %% don't want anyone else to interfere either, so again, we just
    %% wait. But we do update our config node_id map if the
    %% ConfigRemote is coeval with our own.
    case rabbit_clusterer_config:compare(ConfigRemote, Config) of
        coeval -> Config1 = rabbit_clusterer_config:update_node_id(
                              Node, ConfigRemote, NodeID, Config),
                  ok = rabbit_clusterer_config:store_internal(
                         NodeID, Config1),
                  noreply(State #state { config = Config1 });
        _      -> noreply(State #state { nodes = [Node | Nodes] })
    end;
handle_cast({new_config, ConfigRemote, Node},
            State = #state { status = {transitioner, _} }) ->
    %% We have to deal with this case because we could have the
    %% situation where we are blocked in the transitioner waiting for
    %% another node to come up but there really is a younger config
    %% that has become available that we should be transitioning
    %% to. If we don't deal with this we can potentially have a
    %% deadlock.
    noreply(transitioner_event({new_config, ConfigRemote, Node}, State));
handle_cast({new_config, ConfigRemote, Node},
            State = #state { status  = ready,
                             node_id = NodeID,
                             config  = Config }) ->
    %% We a) know what our config really is; b) it's safe to begin
    %% transitions to other configurations.
    Running = rabbit:is_running(),
    case rabbit_clusterer_config:compare(ConfigRemote, Config) of
        younger when not Running ->
                   %% Something has stopped Rabbit. Maybe the
                   %% partition handler. Thus we're going to refuse to
                   %% do anything for the time being.
                   noreply(State);
        younger -> %% Remote is younger. We should switch to it. We
                   %% deliberately do not merge across the configs at
                   %% this stage as it would break is_compatible.
                   %% begin_transition will reboot if necessary.
                   noreply(begin_transition(ConfigRemote, State));
        older   -> ok = send_new_config(Config, Node),
                   noreply(State);
        coeval  -> Config1 = rabbit_clusterer_config:update_node_id(
                               Node, ConfigRemote, NodeID, Config),
                   ok = rabbit_clusterer_config:store_internal(
                          NodeID, Config1),
                   noreply(State #state { config = Config1 });
        invalid -> %% Whilst invalid, the fact is that we are ready,
                   %% so we don't want to disturb that.
                   noreply(State)
    end;

handle_cast(rabbit_booted, State = #state { status = booting }) ->
    %% Note that we don't allow any transition to start whilst we're
    %% booting so it should be safe to assert we can only receive
    %% rabbit_booted when in booting.
    noreply(set_status(ready, State #state { booted = true }));
handle_cast(rabbit_booted, State = #state { status = preboot }) ->
    %% Very likely they forgot to edit the rabbit-server
    %% script. Complain very loudly.
    Msg = "RabbitMQ Clusterer is enabled as a plugin but has "
        "not been started correctly. Terminating RabbitMQ.~n",
    error_logger:error_msg(Msg, []),
    io:format(Msg, []),
    init:stop(),
    {stop, startup_error, State};
handle_cast(rabbit_booted, State = #state { status = ready }) ->
    %% This can happen if the partition handler stopped and then
    %% restarted rabbit.
    noreply(State);

handle_cast(rabbit_boot_failed, State = #state { status = booting }) ->
    ok = rabbit_clusterer_utils:stop_mnesia(),
    noreply(set_status(booting, State));

handle_cast({lock, Locker}, State = #state { comms = undefined }) ->
    gen_server:cast(Locker, {lock_rejected, node()}),
    noreply(State);
handle_cast({lock, Locker}, State = #state { comms = Comms }) ->
    ok = rabbit_clusterer_comms:lock(Locker, Comms),
    noreply(State);
handle_cast({unlock, _Locker}, State = #state { comms = undefined }) ->
    noreply(State);
handle_cast({unlock, Locker}, State = #state { comms = Comms }) ->
    ok = rabbit_clusterer_comms:unlock(Locker, Comms),
    noreply(State);

%% anything else kills us
handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

%%----------------
%% Info
%%----------------

handle_info({transitioner_delay, Event},
            State = #state { status = {transitioner, _} }) ->
    %% A transitioner wanted some sort of timer based callback. Note
    %% it is the transitioner's responsibility to filter out
    %% invalid/outdated etc delayed events.
    noreply(transitioner_event(Event, State));
handle_info({transitioner_delay, _Event}, State) ->
    noreply(State);

%% Monitoring stuff
handle_info({'DOWN', MRef, process, {?SERVER, Node}, _Info},
            State = #state { alive_mrefs = Alive, dead = Dead }) ->
    case lists:delete(MRef, Alive) of
        Alive  -> noreply(State);
        Alive1 -> noreply(ensure_poke_timer(
                            State #state { alive_mrefs = Alive1,
                                           dead        = [Node | Dead] }))
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
    noreply(State #state { dead           = [],
                           alive_mrefs    = Alive1,
                           poke_timer_ref = undefined });
handle_info(poke_the_dead, State) ->
    noreply(State #state { poke_timer_ref = undefined });

%% anything else kills us
handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

%%----------------
%% Rest
%%----------------

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%----------------------------------------------------------------------------
%% Status changes state machine
%%----------------------------------------------------------------------------

%% Here we enforce the state machine of valid changes to status.

%% preboot           -> a transitioner ({transitioner, TKind})
%% preboot           -> shutdown
%% {transitioner, _} -> booting
%% {transitioner, _} -> a transitioner
%% {transitioner, _} -> shutdown
%% booting           -> ready
%% booting           -> booting
%% ready             -> a transitioner
%% ready             -> shutdown

set_status(NewStatus, State = #state { status = Status })
  when ?IS_TRANSITIONER(NewStatus) andalso Status =/= booting ->
    State #state { status = NewStatus };
set_status(booting, State = #state { status  = Status,
                                     booted  = Booted,
                                     node_id = NodeID,
                                     config  = Config })
  when ?IS_TRANSITIONER(Status) orelse Status =:= booting ->
    error_logger:info_msg(
      "Clusterer booting Rabbit into cluster configuration:~n~p~n",
      [rabbit_clusterer_config:to_proplist(NodeID, Config)]),
    PreSleep = Status =:= booting,
    case Booted of
        true  -> ok = rabbit_clusterer_utils:start_rabbit_async(PreSleep);
        false -> ok = rabbit_clusterer_utils:boot_rabbit_async(PreSleep)
    end,
    State #state { status = booting };
set_status(ready, State = #state { status = booting }) ->
    error_logger:info_msg("Cluster achieved and Rabbit running.~n"),
    update_monitoring(State #state { status = ready });
set_status(shutdown, State = #state { status = Status })
  when Status =/= booting ->
    case Status of
        ready -> %% Even though we think we're ready, there might
                 %% still be some rabbit boot actions going on...
                 ok = stop_rabbit();
        _     -> ok
    end,
    error_logger:info_msg("Clusterer stopping node now.~n"),
    init:stop(),
    State #state { status = shutdown }.

noreply(State = #state { status = shutdown }) ->
    {stop, normal, State};
noreply(State) ->
    {noreply, State}.

reply(Reply, State = #state { status = shutdown }) ->
    {stop, normal, Reply, State};
reply(Reply, State) ->
    {reply, Reply, State}.

%%----------------------------------------------------------------------------
%% Changing cluster config
%%----------------------------------------------------------------------------

begin_transition(NewConfig, State = #state { config = Config }) ->
    true = State #state.status =/= booting, %% ASSERTION
    case rabbit_clusterer_config:contains_node(node(), NewConfig) of
        false -> process_transitioner_response({shutdown, NewConfig}, State);
        true  -> begin_transition(
                   rabbit_clusterer_config:is_compatible(NewConfig, Config),
                   rabbit_clusterer_config:transfer_node_ids(Config, NewConfig),
                   State)
    end.

begin_transition(true,     NewConfig, State = #state { status  = ready,
                                                       node_id = NodeID }) ->
    ok = rabbit_clusterer_config:store_internal(NodeID, NewConfig),
    error_logger:info_msg(
      "Clusterer seemlessly transitioned to new configuration:~n~p~n",
      [rabbit_clusterer_config:to_proplist(NodeID, NewConfig)]),
    update_monitoring(State #state { config = NewConfig });
begin_transition(false,    NewConfig, State = #state { status = ready }) ->
    ok = stop_rabbit(),
    join_or_rejoin(join,   NewConfig, State);
begin_transition(true,     NewConfig, State) ->
    join_or_rejoin(rejoin, NewConfig, State);
begin_transition(false,    NewConfig, State) ->
    join_or_rejoin(join,   NewConfig, State).

join_or_rejoin(TKind, NewConfig, State = #state { node_id = NodeID,
                                                  nodes   = Nodes }) ->
    ok = send_new_config(NewConfig, Nodes),
    %% Wipe out alive_mrefs and dead so that if we get DOWN's we don't
    %% care about them.
    {Comms, State1} = fresh_comms(State #state { alive_mrefs = [],
                                                 dead        = [],
                                                 nodes       = [] }),
    process_transitioner_response(
      rabbit_clusterer_transitioner:init(TKind, NodeID, NewConfig, Comms),
      set_status({transitioner, TKind}, State1)).

transitioner_event(Event, State = #state { status = {transitioner, _TKind},
                                           transitioner_state = TState }) ->
    process_transitioner_response(
      rabbit_clusterer_transitioner:event(Event, TState), State).

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
        shutdown -> set_status(shutdown, stop_monitoring(State1))
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
    set_status(shutdown, State1).

fresh_comms(State) ->
    State1 = stop_comms(State),
    {ok, Token} = rabbit_clusterer_comms_sup:start_comms(),
    {Token, State1 #state { comms = Token }}.

stop_comms(State = #state { comms = undefined }) ->
    State;
stop_comms(State = #state { comms = Token }) ->
    ok = rabbit_clusterer_comms:stop(Token),
    State #state { comms = undefined }.

%%----------------------------------------------------------------------------
%% Helpers
%%----------------------------------------------------------------------------

stop_rabbit() ->
    %% This is not idempotent and always assumes that Rabbit should be
    %% running (or at least booting) before being called.
    error_logger:info_msg("Clusterer stopping Rabbit.~n"),
    ok = rabbit:await_startup(),
    ok = rabbit_clusterer_utils:stop_rabbit(),
    ok = rabbit_clusterer_utils:stop_mnesia(),
    ok.

update_monitoring(State = #state { config = ConfigNew,
                                   nodes  = NodesOld }) ->
    State1 = stop_monitoring(State),
    NodesNew = rabbit_clusterer_config:nodenames(ConfigNew) -- [node()],
    ok = send_new_config(ConfigNew, NodesNew -- NodesOld),
    AliveNew = [monitor(process, {?SERVER, N}) || N <- NodesNew],
    State1 #state { nodes       = NodesNew,
                    alive_mrefs = AliveNew}.

stop_monitoring(State = #state { config      = ConfigNew,
                                 nodes       = NodesOld,
                                 alive_mrefs = AliveOld }) ->
    ok = send_new_config(ConfigNew, NodesOld),
    [demonitor(MRef) || MRef <- AliveOld],
    State #state { nodes          = [],
                   alive_mrefs    = [],
                   dead           = [],
                   poke_timer_ref = undefined }.

ensure_poke_timer(State = #state { poke_timer_ref = undefined }) ->
    %% TODO: justify 2000
    State #state { poke_timer_ref =
                       erlang:send_after(2000, self(), poke_the_dead) };
ensure_poke_timer(State) ->
    State.
