-module(rabbit_clusterer_join).

-export([init/3, event/2]).

-record(state, { node_id, config, comms, state }).

-include("rabbit_clusterer.hrl").

init(Config = #config { nodes = Nodes,
                        gospel = Gospel }, NodeID, Comms) ->
    case proplists:get_value(node(), Nodes) of
        disc when length(Nodes) =:= 1 ->
            ok = case Gospel of
                     reset ->
                         rabbit_clusterer_utils:wipe_mnesia();
                     {node, _Node} ->
                         %% _utils:proplist_config_to_record ensures
                         %% if we're here, _Node must be =:= node().
                         rabbit_clusterer_utils:eliminate_mnesia_dependencies()
                 end,
            {success, Config};
        ram when length(Nodes) =:= 1 ->
            {error, ram_only_cluster_config};
        Mode when Mode =/= undefined ->
            request_status(#state { config  = Config,
                                    comms   = Comms,
                                    node_id = NodeID })
    end.

event({comms, {[], _BadNodes}}, State = #state { state = awaiting_status }) ->
    delayed_request_status(State);
event({comms, {Replies, BadNodes}}, State = #state { state   = awaiting_status,
                                                     config  = Config,
                                                     node_id = NodeID }) ->
    {Youngest, OlderThanUs, Statuses} =
        lists:foldr(
          fun ({_Node, preboot}, {YoungestN, OlderThanUsN, StatusesN}) ->
                  {YoungestN, OlderThanUsN, [preboot | StatusesN]};
              ({Node, {ConfigN, StatusN}},
               {YoungestN, OlderThanUsN, StatusesN}) ->
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
                   [StatusN | StatusesN]}
          end, {Config, [], []}, Replies),
    %% Expected keys in StatusDict are:
    %% - preboot:
    %%    clusterer has started, but the boot step not yet hit
    %% - {transitioner, rabbit_clusterer_join} (i.e. ?MODULE):
    %%    it's joining some cluster - blocked in clusterer
    %% - {transitioner, rabbit_clusterer_rejoin}:
    %%    it's rejoining some cluster - blocked in clusterer
    %% - booting:
    %%    clusterer is happy and the rest of rabbit is currently booting
    %% - ready:
    %%    clusterer is happy and enough of rabbit has booted
    %% - pending_shutdown:
    %%    clusterer is waiting for the shutdown timeout and will then exit
    case rabbit_clusterer_utils:compare_configs(Youngest, Config) of
        eq ->
            %% We have the most up to date config. But we must use
            %% Youngest from here on as it has the updated
            %% node_id_maps.
            State1 = State #state { config = Youngest },
            case OlderThanUs of
                [_|_] ->
                    %% Update nodes which are older than us. In
                    %% reality they're likely to receive lots of the
                    %% same update from everyone else, but meh,
                    %% they'll just have to cope.
                    update_remote_nodes(OlderThanUs, State1);
                [] ->
                    %% Everyone here has the same config
                    ReadyNodes = lists:member(ready, Statuses),
                    AllJoining = [{transitioner, ?MODULE}] =:= Statuses,
                    %% ReadyNodes are nodes that are in this cluster
                    %% (well, they could be in any cluster, but seeing
                    %% as we've checked everyone has the same cluster
                    %% config as us, we're sure it really is *this*
                    %% cluster) and are fully up and running.
                    %%
                    %% If ReadyNodes exists, we should just reset and
                    %% join into that, and ignore anything about
                    %% gospel: it's possible that gosple is {node,
                    %% node()} but that, in combination with
                    %% ReadyNodes, suggests that the cluster
                    %% previously existed with an older version of
                    %% 'us': we must have been cleaned out and
                    %% restarted (aka host-name reuse). Here we don't
                    %% care about BadNodes.
                    %%
                    %% If ReadyNodes doesn't exist we can only safely
                    %% proceed if there are no BadNodes, and everyone
                    %% is joining (rather than rejoining)
                    %% i.e. transition module for all is ?MODULE. In
                    %% all other cases, we must wait:
                    %%
                    %% - If BadNodes =/= [] then there may be a node
                    %%   that was cleanly shutdown last with what we
                    %%   think is the current config and so if it was
                    %%   started up, it would rejoin (itself, sort of)
                    %%   and then become ready: we could then sync to
                    %%   it.
                    %%
                    %% - If the transition module is not all ?MODULE
                    %%   then some other nodes must be rejoining. We
                    %%   should wait for them to succeed (or at least
                    %%   change state) because if they do succeed we
                    %%   should sync off them.
                    case ReadyNodes of
                        true ->
                            ok = cluster_with_nodes(Youngest),
                            {success, Youngest};
                        false when AllJoining andalso BadNodes =:= [] ->
                            case maybe_form_new_cluster(Youngest) of
                                true  -> {success, Youngest};
                                false -> delayed_request_status(State1)
                            end;
                        false ->
                            delayed_request_status(State1)
                    end
            end;
        _ ->
            {config_changed, Youngest}
    end;
event({delayed_request_status, Ref},
      State = #state { state = {delayed_request_status, Ref} }) ->
    request_status(State);
event({delayed_request_status, _Ref}, State) ->
    %% ignore it
    {continue, State};
event({request_config, NewNode, NewNodeID, Fun},
      State = #state { config = Config, node_id = NodeID }) ->
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
    NodesNotUs = [ N || {N, _Mode} <- Nodes, N =/= node() ],
    ok = rabbit_clusterer_comms:multi_call(
           NodesNotUs, {request_status, node(), NodeID}, Comms),
    {continue, State #state { state = awaiting_status }}.

delayed_request_status(State) ->
    %% TODO: work out some sensible timeout value
    Ref = make_ref(),
    {sleep, 1000, {delayed_request_status, Ref},
     State #state { state = {delayed_request_status, Ref} }}.

update_remote_nodes(Nodes, State = #state { config = Config, comms = Comms }) ->
    %% Assumption here is Nodes does not contain node(). We
    %% deliberately do this cast out of Comms to preserve ordering of
    %% messages.
    Msg = rabbit_clusterer_coordinator:template_new_config(Config),
    ok = rabbit_clusterer_comms:multi_cast(Nodes, Msg, Comms),
    delayed_request_status(State).

cluster_with_nodes(#config { nodes = Nodes }) ->
    ok = rabbit_clusterer_utils:wipe_mnesia(),
    ok = rabbit_clusterer_utils:configure_cluster(Nodes).

maybe_form_new_cluster(#config { nodes = Nodes, gospel = Gospel }) ->
    MyNode = node(),
    {Wipe, Leader} =
        case Gospel of
            {node, Node} -> {Node =/= MyNode, Node};
            reset -> [Leader1 | _] = lists:usort([N || {N, disc} <- Nodes]),
                     {true, Leader1}
        end,
    case Leader of
        MyNode ->
            ok = case Wipe of
                     true ->
                         rabbit_clusterer_utils:wipe_mnesia();
                     false ->
                         rabbit_clusterer_utils:eliminate_mnesia_dependencies()
                 end,
            ok = case node() of
                     Leader -> rabbit_clusterer_utils:configure_cluster([]);
                     _      -> rabbit_clusterer_utils:configure_cluster(Nodes)
                 end,
            true;
        _ ->
            false
    end.
