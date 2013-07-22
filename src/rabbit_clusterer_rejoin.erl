-module(rabbit_clusterer_rejoin).

-export([init/3, event/2]).

-record(state, { node_id, config, comms }).

-include("rabbit_clusterer.hrl").

init(Config = #config { nodes = Nodes }, NodeID, Comms) ->
    Node = node(),
    %% Check we're actually involved in this
    case proplists:get_value(Node, Nodes) of
        disc when length(Nodes) =:= 1 ->
            %% Simple: we're continuing to cluster with ourself and
            %% we're disk. Don't do a reset. We're done.
            {success, Config};
        Mode when Mode =/= undefined ->
            %% We shouldn't have been able to get into a situation
            %% where we're in a RAM only cluster.
            DiskNodes = [ N || {N, disc} <- Nodes ],
            RamNodes  = [ N || {N, ram} <- Nodes ],
            true = [] =/= DiskNodes, %% ASSERTION
            {ok, {_AllNodes, _DiscNodes, NodesRunningAtShutdown}} =
                rabbit_clusterer_utils:load_last_seen_cluster_state(),
            case NodesRunningAtShutdown of
                [Node] ->
                    %% We're done
                    {success, Config};
                _ ->
                    SurvivingNodes = NodesRunningAtShutdown -- [Node],
                    %%{ok, Ref} = Caster(SurvivingNodes, {rejoin, Config}),
                    {continue, #state { config  = Config,
                                        comms   = Comms,
                                        node_id = NodeID }}
            end
    end.

event({request_config, NewNode, NewNodeID, Fun},
      State = #state { node_id = NodeID, config  = Config }) ->
    {_NodeIDChanged, Config1} =
        rabbit_clusterer_utils:add_node_id(NewNode, NewNodeID, NodeID, Config),
    ok = Fun(Config1),
    {continue, State #state { config = Config1 }}.
