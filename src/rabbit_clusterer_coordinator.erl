-module(rabbit_clusterer_coordinator).

-behaviour(gen_server).

-export([await_coordination/0,
         ready_to_cluster/0,
         send_new_config/2,
         template_new_config/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(IS_TRANSITIONER(X),
        (X =:= rabbit_clusterer_join orelse X =:= rabbit_clusterer_rejoin)).

-record(state, { status,
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
               Nodes, ?SERVER, template_new_config(Config)),
    ok.

template_new_config(Config) ->
    {new_config, Config, node()}.

%%----------------------------------------------------------------------------

init([]) ->
%%    dbg:tracer(), dbg:p(all,c), dbg:tpl(?MODULE, []),
    ok = rabbit_mnesia:ensure_mnesia_dir(),
    {ok, #state { status             = preboot,
                  boot_blocker       = undefined,
                  config             = undefined,
                  transitioner_state = undefined,
                  comms              = undefined,
                  nodes              = [],
                  alive_mrefs        = [],
                  dead               = [],
                  poke_timer_ref     = undefined
                }}.

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
                             status             = {transitioner, TModule},
                             transitioner_state = TState }) ->
    {noreply, process_transitioner_response(
                TModule:event({comms, Result}, TState), State)};
handle_cast({comms, _Comms, _Result}, State) ->
    %% ignore it - either we're not transitioning, or it's from an old
    %% comms pid.
    {noreply, State};
handle_cast({new_config, _ConfigRemote, Node},
            State = #state { status = preboot, nodes  = Nodes }) ->
    %% We can't ignore this: this could be coming from some other node
    %% and their config could be out of date. Our new config may not
    %% mention them - they could be meant to shutdown. So we have to
    %% store them and reply to them when we get going and know what
    %% our config is going to be.
    {noreply, State #state { nodes = [Node | Nodes] }};
handle_cast({new_config, ConfigRemote, Node},
            State = #state { status = Status, config = Config })
  when Status =:= booting orelse Status =:= ready ->
    case rabbit_clusterer_utils:compare_configs(ConfigRemote, Config) of
        gt ->
            case rabbit_clusterer_utils:detect_melisma(ConfigRemote, Config) of
                true ->
                    ConfigRemote1 = rabbit_clusterer_utils:merge_configs(
                                      ConfigRemote, Config),
                    %% It's a "fully compatible" change. We will need
                    %% to update the nodes we're monitoring, and we
                    %% might actually need to shutdown, but basically,
                    %% very minor changes to us - we shouldn't need to
                    %% restart rabbit.  TODO: the above
                    io:format("Shutdown? ~p~n", [ not proplists:is_defined(node(), ConfigRemote1 #config.nodes) ]),
                    {noreply, State #state { config = ConfigRemote1 }};
                false ->
                    %% Ahh, we will need to reboot here. TODO
                    io:format("Reboot required...~n"),
                    {stop, normal, State}
            end;
        lt ->
            %% We are younger. We need to inform Node
            ok = send_new_config(Config, Node),
            {noreply, State};
        _ ->
            %% eq and invalid. We might want eventually to do
            %% something more active on invalid. TODO
            {noreply, State}
    end;
handle_cast({new_config, ConfigRemote, Node},
            State = #state { status = Status, config = Config }) ->
    %% either pending_shutdown or in {transition, _}
    case rabbit_clusterer_utils:compare_configs(ConfigRemote, Config) of
        gt ->
            case rabbit_clusterer_utils:detect_melisma(ConfigRemote, Config) of
                true ->
                    ConfigRemote1 = rabbit_clusterer_utils:merge_configs(
                                      ConfigRemote, Config),
                    %% It's a "fully compatible" change. We will need
                    %% to update the nodes we're monitoring, and we
                    %% might actually need to shutdown, but basically,
                    %% very minor changes to us - we shouldn't need to
                    %% restart rabbit.  TODO: the above
                    io:format("Shutdown? ~p~n", [ not proplists:is_defined(node(), ConfigRemote1 #config.nodes) ]),
                    {noreply, State #state { config = ConfigRemote1 }};
                false ->
                    %% Ahh, we will need to reboot here. TODO
                    io:format("Reboot required...~n"),
                    {stop, normal, State}
            end;
        lt ->
            %% We are younger. We need to inform Node
            ok = send_new_config(Config, Node),
            {noreply, State};
        _ ->
            %% eq and invalid. We might want eventually to do
            %% something more active on invalid. TODO
            {noreply, State}
    end;
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
    State1 = set_status(shutdown, State),
    rabbit:stop(),
    {stop, normal, State1};
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
handle_info({'DOWN', MRef, process, {rabbit_clusterer_coordinator, Node}, _Info},
            State = #state { alive_mrefs = Alive, dead = Dead }) ->
    case lists:delete(MRef, Alive) of
        Alive  -> {noreply, State};
        Alive1 -> {noreply, ensure_poke_timer(
                              State #state { alive_mrefs = Alive1,
                                             dead        = [Node | Dead] })}
    end;
handle_info(poke_the_dead, State = #state { dead = Dead,
                                            alive_mrefs = Alive,
                                            status = Status,
                                            config = Config })
  when Status =:= booting orelse Status =:= running ->
    %% When we're transitioning to something else we don't bother with
    %% the poke as the transitioner will take care of updating nodes
    %% we want to cluster with, and when the transitioner does success
    %% we'll update everyone in the old config too.
    MRefsNew = [monitor(process, {?MODULE, N}) || N <- Dead],
    ok = send_new_config(Config, Dead),
    Alive1 = MRefsNew ++ Alive,
    {noreply, State #state { dead = [], alive_mrefs = Alive1,
                             poke_timer_ref = undefined }};
handle_info(poke_the_dead, State) ->
    {noreply, State #state { poke_timer_ref = undefined }};
handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------
%% Cluster config loading and selection
%%----------------------------------------------------------------------------

begin_clustering(State = #state { status = preboot }) ->
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
    State #state { status = {transitioner, NewStatus} };
set_status(NewStatus, State = #state { status = OldStatus })
  when (OldStatus =:= preboot orelse
        OldStatus =:= pending_shutdown orelse
        OldStatus =:= ready) andalso
       ?IS_TRANSITIONER(NewStatus) ->
    ok = rabbit_clusterer_utils:ensure_start_mnesia(),
    State #state { status = {transitioner, NewStatus} };
set_status(pending_shutdown, State = #state { status = {transitioner, _} }) ->
    ok = rabbit_clusterer_utils:stop_mnesia(),
    %% TODO, erm, this will be mental if there's a boot_blocker!
    %% Looks like it'll give not_started if rabbit's not fully up and
    %% running. Might want an assert that it gives ok iff boot_blocker
    %% =/= undefined.
    application:stop(rabbit),
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

begin_transition(NewConfig, State = #state { config = OldConfig,
                                             nodes  = Nodes }) ->
    TModule = case rabbit_clusterer_utils:detect_melisma(NewConfig, OldConfig) of
                  true  -> rabbit_clusterer_rejoin;
                  false -> rabbit_clusterer_join
              end,
    ok = send_new_config(NewConfig, Nodes),
    %% Don't wipe out Nodes here because we might go through several
    %% different configs as this transition contacts other nodes new
    %% to us. In each case we'll want to inform the nodes we've
    %% previously been in contact with.
    {ok, Comms, State1} = fresh_comms(State #state { alive_mrefs = [],
                                                     dead        = [] }),
    process_transitioner_response(
      TModule:init(NewConfig, Comms), set_status(TModule, State1)).

process_transitioner_response({continue, TState}, State) ->
    State #state { transitioner_state = TState };
process_transitioner_response({continue, Reply, TState}, State) ->
    {Reply, State #state { transitioner_state = TState }};
process_transitioner_response({SuccessOrShutdown, ConfigNew},
                              State = #state { config = ConfigOld })
  when SuccessOrShutdown =:= success orelse SuccessOrShutdown =:= shutdown ->
    %% Both success and shutdown are treated the same as they're exit
    %% nodes from the states of the transitioners. If we've had a
    %% config applied to us that tells us to shutdown, we must record
    %% that config, otherwise we can later be restarted and try to
    %% start up with an outdated config.
    %%
    %% Just to be safe, merge through the configs
    ConfigNew1 = rabbit_clusterer_utils:merge_configs(ConfigNew, ConfigOld),
    ok = write_internal_config(ConfigNew1),
    {ok, State1} = update_monitoring(ConfigNew1, State),
    {ok, State2} = stop_comms(State1 #state { transitioner_state = undefined,
                                              config             = ConfigNew1 }),
    Status = case SuccessOrShutdown of
                 success  -> booting;
                 shutdown -> pending_shutdown
             end,
    reset_shutdown(set_status(Status, State2));
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

update_monitoring(ConfigNew = #config { nodes = NodesNew },
                  State = #state { nodes       = NodesOld,
                                   alive_mrefs = AliveOld }) ->
    MyNode = node(),
    ok = send_new_config(ConfigNew, NodesOld),
    [demonitor(MRef) || MRef <- AliveOld],
    NodeNamesNew = [N || {N, _} <- NodesNew, N =/= MyNode],
    AliveNew = [monitor(process, {?MODULE, N}) || N <- NodeNamesNew],
    {ok, State #state { nodes          = NodeNamesNew,
                        alive_mrefs    = AliveNew,
                        dead           = [],
                        poke_timer_ref = undefined }}.

ensure_poke_timer(State = #state { poke_timer_ref = undefined }) ->
    %% TODO: justify 2000
    State #state { poke_timer_ref =
                       erlang:send_after(2000, self(), poke_the_dead) };
ensure_poke_timer(State) ->
    State.
