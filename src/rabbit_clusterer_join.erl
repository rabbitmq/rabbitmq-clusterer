-module(rabbit_clusterer_join).

-export([init/3, event/2]).

-record(state, { status, node_id, config, comms }).

-include("rabbit_clusterer.hrl").

init(NodeID, Config = #config { nodes = Nodes, gospel = Gospel }, Comms) ->
    MyNode = node(),
    case Nodes of
        [{MyNode, disc}] when Gospel =:= reset ->
            ok = rabbit_clusterer_utils:wipe_mnesia(),
            {success, Config};
        [{MyNode, disc}] ->
            {node, MyNode} = Gospel, %% assertion
            ok = rabbit_clusterer_utils:eliminate_mnesia_dependencies([]),
            {success, Config};
        [_|_] ->
            request_status(#state { node_id = NodeID,
                                    config  = Config,
                                    comms   = Comms })
    end.

event({comms, {[], _BadNodes}}, State = #state { status = awaiting_status }) ->
    delayed_request_status(State);
event({comms, {Replies, BadNodes}}, State = #state { status  = awaiting_status,
                                                     node_id = NodeID,
                                                     config  = Config }) ->
    case rabbit_clusterer_utils:analyse_node_statuses(Replies,
                                                      NodeID, Config) of
        invalid ->
            {invalid_config, Config};
        {Youngest, OlderThanUs, StatusDict} ->
            case rabbit_clusterer_config:compare(Youngest, Config) of
                coeval when OlderThanUs =:= [] ->
                    %% We have the most up to date config. But we must
                    %% use Youngest from here on as it has the updated
                    %% node_id_maps.
                    maybe_join(BadNodes =:= [], dict:fetch_keys(StatusDict),
                               State #state { config = Youngest });
                coeval ->
                    %% Update nodes which are older than us. In
                    %% reality they're likely to receive lots of the
                    %% same update from everyone else, but meh,
                    %% they'll just have to cope.
                    update_remote_nodes(OlderThanUs, Youngest, State);
                younger -> %% cannot be older or invalid
                    {config_changed, Youngest}
            end
    end;
event({delayed_request_status, Ref},
      State = #state { status = {delayed_request_status, Ref} }) ->
    request_status(State);
event({delayed_request_status, _Ref}, State) ->
    %% ignore it
    {continue, State};
event({request_config, NewNode, NewNodeID, Fun},
      State = #state { config = Config, node_id = NodeID }) ->
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

maybe_join(AllGoodNodes, Statuses, State = #state { config = Config }) ->
    %% Everyone here has the same config, thus Statuses can be trusted
    %% as the statuses of all nodes trying to achieve *this* config
    %% and not some other config.
    %%
    %% Expected entries in Statuses are:
    %% - preboot:
    %%    clusterer has started, but the boot step not yet hit
    %% - {transitioner, rabbit_clusterer_join} (i.e. ?MODULE):
    %%    it's joining some cluster - blocked in clusterer
    %% - {transitioner, rabbit_clusterer_rejoin}:
    %%    it's rejoining some cluster - blocked in clusterer
    %% - booting:
    %%    clusterer is happy and the rest of rabbit is currently
    %%    booting
    %% - ready:
    %%    clusterer is happy and enough of rabbit has booted
    %% - pending_shutdown:
    %%    clusterer is waiting for the shutdown timeout and will then
    %%    exit
    ReadyNodes = lists:member(ready, Statuses),
    AllJoining = [{transitioner, ?MODULE}] =:= Statuses,
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
    %% rejoining) i.e. transition module for all is ?MODULE. In all
    %% other cases, we must wait:
    %%
    %% - If BadNodes =/= [] then there may be a node that was cleanly
    %% shutdown last with what we think is the current config and so
    %% if it was started up, it would rejoin (itself, sort of) and
    %% then become ready: we could then sync to it.
    %%
    %% - If the transition module is not all ?MODULE then some other
    %% nodes must be rejoining. We should wait for them to succeed (or
    %% at least change state) because if they do succeed we should
    %% sync off them.
    case ReadyNodes of
        true ->
            ok = cluster_with_nodes(Config),
            {success, Config};
        false when AllJoining andalso AllGoodNodes ->
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
