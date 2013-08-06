-module(rabbit_clusterer_rejoin).

-export([init/3, event/2]).

-record(state, { node_id, config, comms, status, awaiting, joining }).

-include("rabbit_clusterer.hrl").

%% Concerns for this transitioner:
%%
%% - Cluster could have grown or shrunk since we last saw it.
%%
%% - We want to avoid the timeout on mnesia:wait_for_tables, so we
%% need to manage the "dependencies" ourselves.
%%
%% - The disk-nodes-running-when-we-last-shutdown result can be
%% satisfied by any one of those nodes managing to start up. It's
%% certainly not that we depend on *all* of the nodes in there, mearly
%% *any*.
%%
%% - We try to shrink that set as much as possible. If we can get it
%% down to the empty set then we can consider ourselves the "winner"
%% and can start up without waiting for anyone else.
%%
%% - We can remove a node N from that set if:
%%   a) We can show that N (transitively) depends on us (i.e. we have
%%     a cycle) and its other dependencies we can also disregard.
%%   b) We can show that N is currently joining and not
%%     rejoining. Thus it has been reset and we're witnessing hostname
%%     reuse. In this case we must ignore N: if we don't then there's
%%     a risk all the rejoining nodes decide to depend on N, and N (as
%%     it's joining, not rejoining) waits for everyone else. Thus
%%     deadlock.
%%
%% It's tempting to consider a generalisation of (b) where if we see
%% that we depend on a node that is currently rejoining but has a
%% different node id than what we were expecting then it must have
%% been reset since we last saw it and so we shouldn't depend on
%% it. However, this is wrong: the fact that it's rejoining shows that
%% it managed to join (and then be stopped) the cluster after we last
%% saw it. Thus it still has more up-to-date information than us, so
%% we should still depend on it. In this case it should also follow
%% that (a) won't hold for such an N either.
%%
%% Both (a) and (b) require that we can communicate with N. Thus if we
%% have a dependency on a node we can't contact then we can't
%% eliminate it as a dependency, so we just have to wait for either
%% said node to come up, or for someone else to determine they can
%% start.
%%
%% The problem with the cluster shrinking is that we have the
%% possibility of multiple leaders. If A and B both depend on C and
%% the cluster shrinks, losing C, then A and B could both come up,
%% learn of the loss of C and thus declare themselves the leader. It
%% would be possible for both A and B to have *only* C in their
%% initial set of disk-nodes-running-when-we-last-shutdown (in
%% general, the act of adding a node to a cluster and all the nodes
%% rewriting their nodes-running file is non-atomic so we need to be
%% as accomodating as possible here) so neither would feel necessary
%% to wait for each other. Consequently, we have to have some locking
%% to make sure that we don't have multiple leaders (which could cause
%% an mnesia fail-to-merge issue). The rule about locking is that you
%% have to take locks in the same order, and then you can't
%% deadlock. So, we sort all the nodes from the cluster config, and
%% grab locks in order. If a node is down, that's treated as being ok
%% (i.e. you don't abort). You also have to lock yourself. Only when
%% you have all the locks can you actually boot. Why do we lock
%% everyone? Because we can't agree on who to lock. If you tried to
%% pick someone (eg minimum node) then you'd find that could change as
%% other nodes come up or go down, so it's not stable. So lock
%% everyone.
%%
%% Unsurprisingly, this gets a bit more complex. We lock using the
%% comms Pid, and the lock monitors said Pid. This is elegant in that
%% if A locks A and B, and then a new cluster config is applied to A
%% then the comms will be restarted, so the old pid will die, so the
%% locks are released. Similarly, on success, the comms will be
%% stopped, so the lock releases. This is simple and nice. Where it
%% gets slightly more complex is what happens if A locks A and B and
%% then a new config is applied to B. If that were to happen then that
%% would clearly invalidate the config that A is also using. B will
%% forward new config to A too. B and A will both restart their comms,
%% in any other. If B goes first, we don't want B to be held up, so
%% B's lock is actually managed by its comms (i.e. comms both takes
%% the lock and owns the lock). So when B restarts its comms, it's
%% unlocking itself too.

init(Config = #config { nodes = Nodes }, NodeID, Comms) ->
    MyNode = node(),
    case Nodes of
        [{MyNode, disc}] ->
            ok = rabbit_clusterer_utils:eliminate_mnesia_dependencies([]),
            {success, Config};
        [_|_] ->
            request_status(#state { config   = Config,
                                    comms    = Comms,
                                    node_id  = NodeID,
                                    awaiting = undefined,
                                    joining  = [] })
    end.

event({comms, {Replies, BadNodes}}, State = #state { status  = awaiting_status,
                                                     config  = Config,
                                                     node_id = NodeID }) ->
    {Youngest, OlderThanUs, StatusDict} =
        rabbit_clusterer_utils:categorise_configs(Replies, Config, NodeID),
    case Youngest =:= invalid orelse OlderThanUs =:= invalid of
        true ->
            {invalid_config, Config};
        false ->
            case rabbit_clusterer_utils:compare_configs(Youngest, Config) of
                eq ->
                    MyNode = node(),
                    case OlderThanUs of
                        [_|_] ->
                            update_remote_nodes(OlderThanUs, Youngest, State);
                        [] ->
                            %% Everyone who's here is on the same
                            %% config as us. If anyone is running then
                            %% we can just declare success and trust
                            %% mnesia to join into them.
                            State1 = State #state { config = Youngest },
                            #config { nodes = Nodes } = Youngest,
                            SomeoneRunning = dict:is_key(ready, StatusDict),
                            IsRam = ram =:= proplists:get_value(MyNode, Nodes),
                            if
                                SomeoneRunning ->
                                    %% Someone is running, so we
                                    %% should be able to cluster to
                                    %% them.
                                    {success, Youngest};
                                IsRam ->
                                    %% We're ram; can't do anything
                                    %% but wait for someone else
                                    delayed_request_status(State1);
                                true ->
                                    {_All, _Disc, Running} =
                                        rabbit_node_monitor:read_cluster_status(),
                                    DiscSet = ordsets:from_list(
                                                [N || {N, disc} <- Nodes]),
                                    %% Intersect with Running and
                                    %% remove MyNode
                                    DiscRunningSet =
                                        ordsets:del_element(
                                          MyNode,
                                          ordsets:intersection(
                                            DiscSet,
                                            ordsets:from_list(Running))),
                                    BadNodesSet = ordsets:from_list(BadNodes),
                                    Joining =
                                        case dict:find(
                                               {transitioner, rabbit_clusterer_join},
                                               StatusDict) of
                                            {ok, List} -> List;
                                            error      -> []
                                        end,
                                    NotJoiningSet = ordsets:subtract(
                                                      DiscRunningSet, ordsets:from_list(Joining)),
                                    State2 = State1 #state { awaiting = ordsets:to_list(NotJoiningSet),
                                                             joining  = Joining },
                                    case ordsets:is_disjoint(DiscRunningSet, BadNodesSet) of
                                        true ->
                                            %% Everyone we depend on is alive in some form.
                                            case ordsets:size(NotJoiningSet) of
                                                0 ->
                                                    %% We win!
                                                    lock_nodes(State2);
                                                _ ->
                                                    case dict:find({transitioner, ?MODULE}, StatusDict) of
                                                        error ->
                                                            %% No one else is rejoining, nothing we can do but wait.
                                                            delayed_request_status(State2);
                                                        {ok, Rejoining} ->
                                                            collect_dependency_graph(Rejoining, State2)
                                                    end
                                            end;
                                        false ->
                                            %% We might depend on a
                                            %% node in BadNodes. We
                                            %% must wait for it to
                                            %% appear.
                                            delayed_request_status(State2)
                                    end
                            end
                    end;
                _ ->
                    {config_changed, Youngest}
            end
    end;
event({comms, {Replies, BadNodes}}, State = #state { status = awaiting_awaiting,
                                                     awaiting = MyAwaiting }) ->
    InvalidOrUndef = [N || {N, Res} <- Replies,
                           Res =:= invalid orelse Res =:= undefined ],
    case {BadNodes, InvalidOrUndef} of
        {[], []} ->
            MyNode = node(),
            G = digraph:new(),
            try
                %% To win, we need to find that we are in a cycle, and
                %% that cycle, treated as a single unit, has no
                %% outgoing edges. If we detect this, then we can
                %% start to grab locks. In all other cases, we just go
                %% back around.
                %% Add all vertices. This is slightly harder than
                %% you'd imagine because we could have that a node
                %% depends on a node which we've not queried yet
                %% (because it's a badnode).
                Replies1 = [{MyNode, MyAwaiting} | Replies],
                Nodes = lists:usort(
                          lists:append(
                            [[N|Awaiting] || {N, Awaiting} <- Replies1])),
                [digraph:add_vertex(G, N) || N <- Nodes],
                [digraph:add_edge(G, N, T) || {N, Awaiting} <- Replies1,
                                              T <- Awaiting],
                CSC = digraph_utils:cyclic_strong_components(G),
                [OurComponent] = [C || C <- CSC, lists:member(MyNode, C)],
                %% Detect if there are any outbound edges from this
                %% component
                case [N || V <- OurComponent,
                           N <- digraph:out_neighbours(G, V),
                           not lists:member(N, OurComponent) ] of
                    [] -> %% We appear to be in the "root"
                          %% component. Begin the fight.
                          lock_nodes(State);
                    _  -> delayed_request_status(State)
                end
            after
                true = digraph:delete(G)
            end;
        _ ->
            %% Go around again...
            delayed_request_status(State)
    end;
event({comms, lock_rejected}, State = #state { status = awaiting_lock }) ->
    %% Oh, well let's just wait and try again. Something must have
    %% changed.
    delayed_request_status(State);
event({comms, lock_ok}, #state { status  = awaiting_lock,
                                 config  = Config,
                                 joining = Joining }) ->
    ok = rabbit_clusterer_utils:eliminate_mnesia_dependencies(Joining),
    {success, Config};
event({delayed_request_status, Ref},
      State = #state { status = {delayed_request_status, Ref} }) ->
    request_status(State);
event({delayed_request_status, _Ref}, State) ->
    %% ignore it
    {continue, State};
event({request_config, NewNode, NewNodeID, Fun},
      State = #state { node_id = NodeID, config  = Config }) ->
    %% Right here we could have a node that we're dependent on being
    %% reset.
    {NodeIDChanged, Config1} =
        rabbit_clusterer_utils:add_node_id(NewNode, NewNodeID, NodeID, Config),
    ok = Fun(Config1),
    case NodeIDChanged of
        true  -> {config_changed, Config1};
        false -> {continue, State #state { config = Config1 }}
    end;
event({request_awaiting, Fun}, State = #state { awaiting = Awaiting }) ->
    ok = Fun(Awaiting),
    {continue, State};
event({new_config, ConfigRemote, Node},
      State = #state { config = Config = #config { nodes = Nodes } }) ->
    case rabbit_clusterer_utils:compare_configs(ConfigRemote, Config) of
        lt -> ok = rabbit_clusterer_coordinator:send_new_config(Config, Node),
              {continue, State};
        gt -> ok = rabbit_clusterer_coordinator:send_new_config(
                     ConfigRemote,
                     [N || {N, _} <- Nodes, N =/= node(), N =/= Node ]),
              {config_changed, ConfigRemote};
        _  -> %% ignore
              {continue, State}
    end.

collect_dependency_graph(RejoiningNodes, State = #state { comms = Comms }) ->
    ok = rabbit_clusterer_comms:multi_call(
           RejoiningNodes, {{transitioner, ?MODULE}, request_awaiting}, Comms),
    {continue, State #state { status = awaiting_awaiting }}.


request_status(State = #state { comms   = Comms,
                                node_id = NodeID,
                                config  = #config { nodes = Nodes } }) ->
    MyNode = node(),
    NodesNotUs = [ N || {N, _Mode} <- Nodes, N =/= MyNode ],
    ok = rabbit_clusterer_comms:multi_call(
           NodesNotUs, {request_status, MyNode, NodeID}, Comms),
    {continue, State #state { status = awaiting_status }}.

delayed_request_status(State) ->
    %% TODO: work out some sensible timeout value
    Ref = make_ref(),
    {sleep, 1000, {delayed_request_status, Ref},
     State #state { status = {delayed_request_status, Ref} }}.

update_remote_nodes(Nodes, Config, State = #state { comms = Comms }) ->
    %% Assumption here is Nodes does not contain node(). We
    %% deliberately do this cast out of Comms to preserve ordering of
    %% messages.
    Msg = rabbit_clusterer_coordinator:template_new_config(Config),
    ok = rabbit_clusterer_comms:multi_cast(Nodes, Msg, Comms),
    delayed_request_status(State).

lock_nodes(State = #state { comms = Comms, config = Config }) ->
    ok = rabbit_clusterer_comms:lock_nodes(
           rabbit_clusterer_utils:nodenames(Config), Comms),
    {continue, State #state { status = awaiting_lock }}.
