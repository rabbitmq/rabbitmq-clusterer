-module(rabbit_clusterer_transitioner).

-export([init/4, event/2]).

-record(state, { kind, status, node_id, config, comms, awaiting, joining }).

-include("rabbit_clusterer.hrl").

%% Concerns for join: TODO explain
%%
%%
%% Concerns for rejoin:
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
%% Unsurprisingly, this gets a bit more complex. The lock is the Comms
%% Pid, and the lock is taken by Comms Pids. The lock monitors the
%% taker. This is elegant in that if A locks A and B, and then a new
%% cluster config is applied to A then A's comms will be restarted, so
%% the old comms Pid will die, so the locks are released. Similarly,
%% on success, the comms will be stopped, so the lock releases. This
%% is simple and nice. Where it gets slightly more complex is what
%% happens if A locks A and B and then a new config is applied to
%% B. If that were to happen then that would clearly invalidate the
%% config that A is also using. B will forward new config to A too. B
%% and A will both restart their comms, in any order. If B goes first,
%% we don't want B to be held up, so as B will get a new comms, it
%% also gets a new lock as the lock is the comms Pid itself. So when B
%% restarts its comms, it's unlocking itself too.

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

init(Kind, NodeID, Config = #config { nodes = Nodes }, Comms) ->
    MyNode = node(),
    Gospel = Config #config.gospel,
    case Nodes of
        [{MyNode, disc}] when Kind =:= join andalso Gospel =:= reset ->
            ok = rabbit_clusterer_utils:wipe_mnesia(),
            {success, Config};
        [{MyNode, disc}] ->
            %% TODO: the following assertion came from 'join'; is it
            %% also valid for 'rejoin'?
            %% {node, MyNode} = Gospel, %% ASSERTION
            ok = rabbit_clusterer_utils:eliminate_mnesia_dependencies([]),
            {success, Config};
        [_|_] ->
            request_status(#state { kind    = Kind,
                                    node_id = NodeID,
                                    config  = Config,
                                    comms   = Comms,
                                    awaiting = undefined,
                                    joining  = [] })
    end.

event({comms, {[], _BadNodes}}, State = #state { kind   = join,
                                                 status = awaiting_status }) ->
    delayed_request_status(State);
event({comms, {Replies, BadNodes}}, State = #state { kind    = Kind,
                                                     status  = awaiting_status,
                                                     node_id = NodeID,
                                                     config  = Config }) ->
    case analyse_node_statuses(Replies, NodeID, Config) of
        invalid ->
            {invalid_config, Config};
        {Youngest, OlderThanUs, StatusDict} ->
            case rabbit_clusterer_config:compare(Youngest, Config) of
                coeval when OlderThanUs =:= [] ->
                    %% We have the most up to date config. But we must
                    %% use Youngest from here on as it has the updated
                    %% node_id_maps.
                    (case Kind of
                         join   -> fun maybe_join/3;
                         rejoin -> fun maybe_rejoin/3
                     end)(BadNodes, StatusDict,
                          State #state { config = Youngest });
                coeval ->
                    %% Update nodes which are older than us. In
                    %% reality they're likely to receive lots of the
                    %% same update from everyone else, but meh,
                    %% they'll just have to cope.
                    update_remote_nodes(OlderThanUs, Youngest,
                                        State #state { config = Youngest });
                younger -> %% cannot be older or invalid
                    {config_changed, Youngest}
            end
    end;
event({comms, {Replies, BadNodes}}, State = #state { kind     = rejoin,
                                                     status   = awaiting_awaiting,
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

event({comms, lock_rejected}, State = #state { kind   = rejoin,
                                               status = awaiting_lock }) ->
    %% Oh, well let's just wait and try again. Something must have
    %% changed.
    delayed_request_status(State);
event({comms, lock_ok}, #state { kind    = rejoin,
                                 status  = awaiting_lock,
                                 config  = Config,
                                 joining = Joining }) ->
    ok = rabbit_clusterer_utils:eliminate_mnesia_dependencies(Joining),
    {success, Config};

event({request_awaiting, Fun}, State = #state { kind     = rejoin,
                                                awaiting = Awaiting }) ->
    ok = Fun(Awaiting),
    {continue, State};

event({delayed_request_status, Ref},
      State = #state { status = {delayed_request_status, Ref} }) ->
    request_status(State);
event({delayed_request_status, _Ref}, State) ->
    %% ignore it
    {continue, State};

event({request_config, NewNode, NewNodeID, Fun},
      State = #state { node_id = NodeID, config = Config }) ->
    %% Right here we could have a node that we're dependent on being
    %% reset.
    {NodeIDChanged, Config1} =
        rabbit_clusterer_config:add_node_id(NewNode, NewNodeID, NodeID, Config),
    ok = Fun(Config1),
    case NodeIDChanged of
        true  -> {config_changed, Config1};
        false -> {continue, State #state { config = Config1 }}
    end;

event({new_config, ConfigRemote, Node},
      State = #state { node_id = NodeID, config = Config }) ->
    case rabbit_clusterer_config:compare(ConfigRemote, Config) of
        older   -> ok = rabbit_clusterer_coordinator:send_new_config(Config, Node),
                   {continue, State};
        younger -> %% Here we also need to make sure we forward this to
                   %% anyone we're currently trying to cluster with:
                   %% the fact that we're about to change which config
                   %% we're using clearly invalidates our current
                   %% config and it's not just us using this
                   %% config. We send from here and not comms as we're
                   %% about to kill off comms anyway so there's no
                   %% ordering issues to consider.
                   ok = rabbit_clusterer_coordinator:send_new_config(
                          ConfigRemote,
                          rabbit_clusterer_config:nodenames(Config) --
                              [node(), Node]),
                   {config_changed, ConfigRemote};
        coeval  -> Config1 = rabbit_clusterer_config:update_node_id(
                               Node, ConfigRemote, NodeID, Config),
                   {continue, State #state { config = Config1 }};
        invalid -> %% ignore
                   {continue, State}
    end.

%%----------------------------------------------------------------------------
%% 'join' helpers
%%----------------------------------------------------------------------------

maybe_join(BadNodes, StatusDict, State = #state { config = Config }) ->
    %% Everyone here has the same config, thus Statuses can be trusted
    %% as the statuses of all nodes trying to achieve *this* config
    %% and not some other config.
    %%
    %% Expected entries in Statuses are:
    %% - preboot:
    %%    Clusterer has started, but the boot step not yet hit
    %% - {transitioner, join}:
    %%    it's joining some cluster - blocked in Clusterer
    %% - {transitioner, rejoin}:
    %%    it's rejoining some cluster - blocked in Clusterer
    %% - booting:
    %%    Clusterer is happy and the rest of rabbit is currently
    %%    booting
    %% - ready:
    %%    Clusterer is happy and enough of rabbit has booted
    %% - pending_shutdown:
    %%    Clusterer is waiting for the shutdown timeout and will then
    %%    exit
    Statuses   = dict:fetch_keys(StatusDict),
    ReadyNodes = lists:member(ready, Statuses),
    AllJoining = [{transitioner, join}] =:= Statuses,
    %% ReadyNodes are nodes that are in this cluster (well, they could
    %% be in any cluster, but seeing as we've checked everyone has the
    %% same cluster config as us, we're sure it really is *this*
    %% cluster) and are fully up and running.
    %%
    %% If ReadyNodes exists, we should just reset and join into that,
    %% and ignore anything about gospel: it's possible that gospel is
    %% {node, node()} but that, in combination with ReadyNodes,
    %% suggests that the cluster previously existed with an older
    %% version of 'us': we must have been cleaned out and restarted
    %% (aka host-name reuse). Here we don't care about BadNodes.
    %%
    %% If ReadyNodes doesn't exist we can only safely proceed if there
    %% are no BadNodes, and everyone is joining (rather than
    %% rejoining) i.e. transitioner kind for all is 'join'. In all
    %% other cases, we must wait:
    %%
    %% - If BadNodes =/= [] then there may be a node that was cleanly
    %% shutdown last with what we think is the current config and so
    %% if it was started up, it would rejoin (itself, sort of) and
    %% then become ready: we could then sync to it.
    %%
    %% - If the transitioner kind is not all 'join' then some other
    %% nodes must be rejoining. We should wait for them to succeed (or
    %% at least change state) because if they do succeed we should
    %% sync off them.
    case ReadyNodes of
        true ->
            ok = cluster_with_nodes(Config),
            {success, Config};
        false when AllJoining andalso BadNodes =:= [] ->
            case maybe_form_new_cluster(Config) of
                true  -> {success, Config};
                false -> delayed_request_status(State)
            end;
        false ->
            delayed_request_status(State)
    end.

cluster_with_nodes(#config { nodes = Nodes }) ->
    ok = rabbit_clusterer_utils:wipe_mnesia(),
    ok = rabbit_clusterer_utils:configure_cluster(Nodes).

maybe_form_new_cluster(Config = #config { nodes = Nodes, gospel = Gospel }) ->
    %% Is it necessary to limit the election of a leader to disc
    %% nodes? No: we're only here when we have everyone in the cluster
    %% joining, so we know we wouldn't be creating a RAM-node-only
    %% cluster. Given that we enforce that the cluster config must
    %% have at least one disc node in it anyway, it's safe to allow a
    %% RAM node to lead. However, I'm not 100% sure that the rest of
    %% rabbit/mnesia likes that, so we leave in the 'disc'
    %% filter. This might get reviewed in QA.
    MyNode = node(),
    {Wipe, Leader} =
        case Gospel of
            {node, Node} ->
                {Node =/= MyNode, Node};
            reset ->
                {true, lists:min(rabbit_clusterer_config:disc_nodenames(Config))}
        end,
    case Leader of
        MyNode ->
            ok =
                case Wipe of
                    true ->
                        rabbit_clusterer_utils:wipe_mnesia();
                    false ->
                        rabbit_clusterer_utils:eliminate_mnesia_dependencies([])
                end,
            ok = rabbit_clusterer_utils:configure_cluster(
                   [{MyNode, orddict:fetch(MyNode, Nodes)}]),
            true;
        _ ->
            false
    end.

%%----------------------------------------------------------------------------
%% 'rejoin' helpers
%%----------------------------------------------------------------------------

collect_dependency_graph(RejoiningNodes, State = #state { comms = Comms }) ->
    ok = rabbit_clusterer_comms:multi_call(
           RejoiningNodes, {{transitioner, rejoin}, request_awaiting}, Comms),
    {continue, State #state { status = awaiting_awaiting }}.

maybe_rejoin(BadNodes, StatusDict,
             State = #state { config = Config = #config { nodes = Nodes } }) ->
    %% Everyone who's here is on the same config as us. If anyone is
    %% running then we can just declare success and trust mnesia to
    %% join into them.
    MyNode = node(),
    SomeoneRunning = dict:is_key(ready, StatusDict),
    IsRam = ram =:= orddict:fetch(MyNode, Nodes),
    if
        SomeoneRunning ->
            %% Someone is running, so we should be able to cluster to
            %% them.
            {success, Config};
        IsRam ->
            %% We're ram; can't do anything but wait for someone else
            delayed_request_status(State);
        true ->
            {_All, _Disc, Running} = rabbit_node_monitor:read_cluster_status(),
            DiscSet = ordsets:from_list(
                        rabbit_clusterer_config:disc_nodenames(Config)),
            %% Intersect with Running and remove MyNode
            DiscRunningSet =
                ordsets:del_element(
                  MyNode, ordsets:intersection(
                            DiscSet, ordsets:from_list(Running))),
            BadNodesSet = ordsets:from_list(BadNodes),
            Joining = case dict:find({transitioner, join}, StatusDict) of
                          {ok, List} -> List;
                          error      -> []
                      end,
            NotJoiningSet = ordsets:subtract(
                              DiscRunningSet, ordsets:from_list(Joining)),
            State1 = State #state { awaiting = ordsets:to_list(NotJoiningSet),
                                    joining  = Joining },
            case ordsets:is_disjoint(DiscRunningSet, BadNodesSet) of
                true ->
                    %% Everyone we depend on is alive in some form.
                    case {ordsets:size(NotJoiningSet),
                          dict:find({transitioner, rejoin}, StatusDict)} of
                        {0, _} ->
                            %% We win!
                            lock_nodes(State1);
                        {_, error} ->
                            %% No one else is rejoining, nothing we
                            %% can do but wait.
                            delayed_request_status(State1);
                        {_, {ok, Rejoining}} ->
                            collect_dependency_graph(Rejoining, State1)
                    end;
                false ->
                    %% We might depend on a node in BadNodes. We must
                    %% wait for it to appear.
                    delayed_request_status(State1)
            end
    end.

lock_nodes(State = #state { comms = Comms, config = Config }) ->
    ok = rabbit_clusterer_comms:lock_nodes(
           rabbit_clusterer_config:nodenames(Config), Comms),
    {continue, State #state { status = awaiting_lock }}.

%%----------------------------------------------------------------------------
%% common helpers
%%----------------------------------------------------------------------------

request_status(State = #state { node_id = NodeID,
                                config  = Config,
                                comms   = Comms }) ->
    MyNode = node(),
    NodesNotUs = rabbit_clusterer_config:nodenames(Config) -- [MyNode],
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

%% The input is a k/v list of nodes and their config+status tuples (or
%% the atom 'preboot' if the node is in the process of starting up),
%% plus the local node's id and config.
%%
%% Returns a tuple containing
%% 1) the youngest config of all, with an enriched map_node_id
%% 2) a list of nodes operating with configs older than the local node's
%% 3) a dict mapping status to lists of nodes
analyse_node_statuses(NodeConfigStatusList, NodeID, Config) ->
    case lists:foldr(
           fun (Elem, Acc) -> analyse_node_status(Config, Elem, Acc) end,
           {Config, [], [], dict:new()}, NodeConfigStatusList) of
        invalid ->
            invalid;
        {Youngest, Older, IDs, Status} ->
            %% We want to make sure anything that we had in Config
            %% that does not exist in IDs is still maintained.
            YoungestOrigMap = rabbit_clusterer_config:transfer_map(Config,
                                                                   Youngest),
            {rabbit_clusterer_config:add_node_ids(IDs, NodeID, YoungestOrigMap),
             Older, Status}
    end.

analyse_node_status(_Config, _Reply, invalid) ->
    invalid;
analyse_node_status(_Config, {Node, preboot},
                    {YoungestN, OlderN, IDsN, StatusesN}) ->
    {YoungestN, OlderN, IDsN, dict:append(preboot, Node, StatusesN)};
analyse_node_status(Config, {Node, {ConfigN, StatusN}},
                    {YoungestN, OlderN, IDsN, StatusesN}) ->
    VsYoungest = rabbit_clusterer_config:compare(ConfigN, YoungestN),
    VsConfig   = rabbit_clusterer_config:compare(ConfigN, Config),
    case VsYoungest =:= invalid orelse VsConfig =:= invalid of
        true  -> invalid;
        false -> YoungestN1 = case VsYoungest of
                                  younger -> ConfigN;
                                  _       -> YoungestN
                              end,
                 OlderN1    = case VsConfig   of
                                  older   -> [Node | OlderN];
                                  _       -> OlderN
                              end,
                 NodeIDN = orddict:fetch(Node, ConfigN #config.map_node_id),
                 IDsN1 = [{Node, NodeIDN} | IDsN],
                 StatusesN1 = dict:append(StatusN, Node, StatusesN),
                 {YoungestN1, OlderN1, IDsN1, StatusesN1}
    end.
