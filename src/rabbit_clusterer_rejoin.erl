-module(rabbit_clusterer_rejoin).

-export([init/3, event/2]).

-record(state, { node_id, config, comms, status }).

-include("rabbit_clusterer.hrl").

%% In this transitioner we want to avoid the timeout on
%% mnesia:wait_for_tables so we need to figure out who we depend on to
%% rejoin and then wait for them to be running. The only substantial
%% complication comes from if some node we're dependent on gets reset.
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
