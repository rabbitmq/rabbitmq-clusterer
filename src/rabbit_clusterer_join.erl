-module(rabbit_clusterer_join).

-export([init/3, event/2]).

-record(state, { config, node_id, comms }).

-include("rabbit_clusterer.hrl").

init(Config = #config { nodes = Nodes }, NodeID, Comms) ->
    %% 1. Check we're actually involved in this
    case proplists:get_value(node(), Nodes) of
        undefined ->
            %% Oh. We're not in there...
            {shutdown, Config};
        disc when length(Nodes) =:= 1 ->
            %% Simple: we're just clustering with ourself and we're
            %% disk. Just do a reset and we're done.
            ok = rabbit_mnesia:force_reset(),
            {success, Config};
        ram when length(Nodes) =:= 1 ->
            {error, ram_only_cluster_config};
        _ ->
            NodesNotUs = [ N || {N, _Mode} <- Nodes,
                                N =/= node() ],
            %% Right, time to consult with our neighbours.
            {continue, #state { config  = Config,
                                node_id = NodeID,
                                comms   = Comms }}
    end.

event(Event, Config) ->
    {continue, Config}.
