-module(rabbit_clusterer_rejoin).

-export([init/2, event/2]).

-record(state, { config, comms }).

-include("rabbit_clusterer.hrl").

init(Config = #config { nodes = Nodes }, Comms) ->
    Node = node(),
    %% Check we're actually involved in this
    case proplists:get_value(Node, Nodes) of
        undefined ->
            %% Oh. We're not in there...
            {shutdown, Config};
        disc when length(Nodes) =:= 1 ->
            %% Simple: we're continuing to cluster with ourself and
            %% we're disk. Don't do a reset. We're done.
            {success, Config};
        Mode ->
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
                                        comms   = Comms }}
            end
    end.

event({request_config, Node, NodeID}, State = #state { config = Config }) ->
    {_NodeIDChanged, Config1} =
        rabbit_clusterer_utils:add_node_id(Node, NodeID, Config),
    {continue, Config1, State #state { config = Config1 }}.
