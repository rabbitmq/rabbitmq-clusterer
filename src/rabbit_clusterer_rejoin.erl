-module(rabbit_clusterer_rejoin).

-export([init/3, event/2]).

-record(state, { node_id, config, comms, status }).

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
%% you have all the locks can you actually boot.
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
            {success, Config};
        [_|_] ->
            request_status(#state { config  = Config,
                                    comms   = Comms,
                                    node_id = NodeID })
    end.

event({comms, {Replies, BadNodes}}, State = #state { status  = awaiting_status,
                                                     comms   = Comms,
                                                     config  = Config,
                                                     node_id = NodeID }) ->
    {Youngest, OlderThanUs, StatusDict} =
        lists:foldr(
          fun ({Node, preboot}, {YoungestN, OlderThanUsN, StatusDictN}) ->
                  {YoungestN, OlderThanUsN, dict:append(preboot, Node, StatusDictN)};
              ({Node, {ConfigN, StatusN}},
               {YoungestN, OlderThanUsN, StatusDictN}) ->
                  {case rabbit_clusterer_utils:compare_configs(ConfigN,
                                                               YoungestN) of
                       invalid -> %% TODO tidy this up - probably shouldn't be a throw.
                                  throw("Configs with same version numbers but semantically different");
                       lt      -> YoungestN;
                       _       -> %% i.e. gt *or* eq - must merge if eq too!
                                  rabbit_clusterer_utils:merge_configs(
                                    NodeID, ConfigN, YoungestN)
                   end,
                   case rabbit_clusterer_utils:compare_configs(ConfigN,
                                                               Config) of
                       invalid -> %% TODO tidy this up - probably shouldn't be a throw.
                                  throw("Configs with same version numbers but semantically different");
                       lt      -> [Node | OlderThanUsN];
                       _       -> OlderThanUsN
                   end,
                   dict:append(StatusN, Node, StatusDictN)}
          end, {Config, [], dict:new()}, Replies),
    case rabbit_clusterer_utils:compare_configs(Youngest, Config) of
        eq ->
            MyNode = node(),
            State1 = State #state { config = Youngest },
            case OlderThanUs of
                [_|_] ->
                    update_remote_nodes(OlderThanUs, State1);
                [] ->
                    %% Everyone who's here is on the same config as
                    %% us. If anyone is running then we can just
                    %% declare success and trust mnesia to join into
                    %% them.
                    #config { nodes = Nodes } = Youngest,
                    SomeoneRunning = dict:is_key(ready, StatusDict),
                    IsRam = ram =:= proplists:get_value(MyNode, Nodes),
                    if
                        SomeoneRunning ->
                            %% Someone is running, so we should be
                            %% able to cluster to them.
                            {success, Youngest};
                        IsRam ->
                            %% We're ram; can't do anything but wait
                            %% for someone else
                            delayed_request_status(State1);
                        true ->
                            {_All, Disc, Running} = rabbit_node_monitor:read_cluster_status(),
                            DiscRunning = ordsets:to_list(
                                            ordsets:intersection(
                                              ordsets:from_list(Disc),
                                              ordsets:from_list(Running))) -- [MyNode],
                            %% We want to shrink DiscRunning as much
                            %% as possible. It's not so much that we
                            %% depend on *all* the elements in
                            %% DiscRunning, it's more that we depend
                            %% on *any* of those elements. If we can
                            %% get it down to [] then we can declare
                            %% ourselves the leader and start up. If
                            %% we can't, we have to wait.
                            %%
                            %% There are two possible ways to remove
                            %% a given node N from DiscRunning:
                            %%
                            %% a) Show that N (transitively) depends
                            %%   on us (i.e. we have a cycle) (and
                            %%   other nodes that we can eliminate)
                            %%   and that MyNode < N (i.e. we can only
                            %%   have one winner of this cycle)
                            %%
                            %% b) Show that N is currently joining
                            %%   (instead of rejoining). If it's
                            %%   joining then it's been reset, so we
                            %%   can't possibly depend on it.
                            %%
                            %% Both of these require that we either
                            %% can or have communicated with the nodes
                            %% in DiscRunning. Therefore if any of the
                            %% nodes in DiscRunning are also in
                            %% BadNodes then there's no point doing
                            %% any further work right now.
                            Joining = case dict:find(rabbit_clusterer_join, StatusDict) of
                                          {ok, List} -> List;
                                          error      -> []
                                      end,
                            DiscRunning1 = DiscRunning -- Joining,
                            DiscRunningAllAlive = length(DiscRunning1) =:=
                                length(DiscRunning1 -- BadNodes),
                            case DiscRunning1 of
                                [] ->
                                    %% We have a race here. There
                                    %% could be multiple nodes both of
                                    %% which "only" depend on nodes
                                    %% that are currently joining
                                    %% (i.e. they've been reset). This
                                    %% code, as it stands, would start
                                    %% both up as the leader.
                                    ok = rabbit_clusterer_utils:eliminate_mnesia_dependencies(),
                                    {success, Youngest};
                                [_|_] when DiscRunningAllAlive ->
                                    %% Need to do cycle detection
                                    ko;
                                _ ->
                                    %% We might depend on a node in
                                    %% BadNodes. We must wait for it
                                    %% to appear.
                                    delayed_request_status(State1)
                            end
                    end
            end;
        _ ->
            {config_changed, Youngest}
    end;
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
    {_NodeIDChanged, Config1} =
        rabbit_clusterer_utils:add_node_id(NewNode, NewNodeID, NodeID, Config),
    ok = Fun(Config1),
    {continue, State #state { config = Config1 }};
event({new_config, ConfigRemote, Node}, State = #state { config = Config }) ->
    case rabbit_clusterer_utils:compare_configs(ConfigRemote, Config) of
        lt -> ok = rabbit_clusterer_coordinator:send_new_config(Config, Node),
              {continue, State};
        gt -> {config_changed, ConfigRemote};
        _  -> %% ignore
              {continue, State}
    end.

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

update_remote_nodes(Nodes, State = #state { config = Config, comms = Comms }) ->
    %% Assumption here is Nodes does not contain node(). We
    %% deliberately do this cast out of Comms to preserve ordering of
    %% messages.
    Msg = rabbit_clusterer_coordinator:template_new_config(Config),
    ok = rabbit_clusterer_comms:multi_cast(Nodes, Msg, Comms),
    delayed_request_status(State).
