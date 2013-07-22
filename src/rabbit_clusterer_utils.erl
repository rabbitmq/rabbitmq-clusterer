-module(rabbit_clusterer_utils).

-include("rabbit_clusterer.hrl").

-export([default_config/0,
         proplist_config_to_record/1,
         record_config_to_proplist/2,
         load_last_seen_cluster_state/0,
         compare_configs/2,
         wipe_mnesia/0,
         merge_configs/3,
         add_node_id/4,
         eliminate_mnesia_dependencies/0,
         configure_cluster/1,
         stop_mnesia/0,
         stop_rabbit/0,
         ensure_start_mnesia/0,
         detect_melisma/2,
         node_in_config/1
        ]).

default_config() ->
    proplist_config_to_record(
      [{nodes, [{node(), disc}]},
       {version, 0},
       {gospel, {node, node()}},
       {shutdown_timeout, infinity}
      ]).

required_keys() ->
    [nodes, version, gospel, shutdown_timeout].

optional_keys() ->
    NodeID = create_node_id(),
    [{minor_version, 0},
     {map_node_id, orddict:from_list([{node(), NodeID}])},
     {map_id_node, orddict:from_list([{NodeID, node()}])},
     {node_id, NodeID}].

%% Generally we're somewhat brutal in the validation currently - we
%% just blow up whenever we encounter something that's not right. This
%% can be improved later. In general, this code could gain a lot from
%% some of the monadic code in the shovel config work.
proplist_config_to_record(Proplist) when is_list(Proplist) ->
    Keys = proplists:get_keys(Proplist),
    [] = required_keys() -- Keys, %% ASSERTION
    Proplist1 = ensure_entries(optional_keys(), Proplist),
    [] = (proplists:get_keys(Proplist1) -- required_keys())
        -- proplists:get_keys(optional_keys()), %% ASSERTION
    Config = #config {},
    Fields = record_info(fields, config),
    {_Pos, Config1 = #config { nodes = Nodes }} =
        lists:foldl(fun (FieldName, {Pos, ConfigN}) ->
                            Value = proplists:get_value(FieldName, Proplist1),
                            {Pos + 1, setelement(Pos, ConfigN, Value)}
                    end, {2, Config}, Fields),
    Nodes1 = normalise_nodes(Nodes),
    true = [] =/= [N || {N, disc} <- Nodes1], %% ASSERTION
    NodeID = proplists:get_value(node_id, Proplist1),
    true = NodeID =/= undefined, %% ASSERTION
    Config2 = #config { gospel = Gospel } = Config1 #config { nodes = Nodes1 },
    case Gospel of
        reset        -> {NodeID, Config2};
        {node, Node} -> disc = proplists:get_value(Node, Nodes1), %% ASSERTION
                        {NodeID, Config2}
    end.

ensure_entries(Entries, Proplist) ->
    lists:foldr(fun ({Key, _Default} = E, ProplistN) ->
                        case proplists:is_defined(Key, ProplistN) of
                            true  -> ProplistN;
                            false -> [E | ProplistN]
                        end
                end, Proplist, Entries).

normalise_nodes(Nodes) when is_list(Nodes) ->
    lists:usort(
      lists:map(fun ({Node, disc} = E) when is_atom(Node) -> E;
                    ({Node, disk})     when is_atom(Node) -> {Node, disc};
                    (Node)             when is_atom(Node) -> {Node, disc};
                    ({Node, ram} = E)  when is_atom(Node) -> E
                end, Nodes)).

%% Sod it - we just regenerate map_id_node rather than trying to
%% tidy. Easy to ensure correctness.
tidy_node_id_maps(NodeID, Config = #config { nodes = Nodes,
                                             map_node_id = NodeToID }) ->
    %% We always remove ourself from the maps to take into account our
    %% own node_id has changed (and then add ourself back in).
    MyNode = node(),
    NodeNames = [N || {N, _} <- Nodes, N =/= MyNode],
    NodesToRemove = orddict:fetch_keys(NodeToID) -- NodeNames,
    NodeToID1 = lists:foldr(fun orddict:erase/2, NodeToID, NodesToRemove),
    %% There's a possibility that we need to add in the mapping for
    %% the local node (consider that a previous config didn't include
    %% ourself, but a new one does).
    NodeToID2 = case proplists:is_defined(node(), Nodes) of
                    true  -> orddict:store(node(), NodeID, NodeToID1);
                    false -> NodeToID1
                end,
    IDToNode = orddict:fold(fun (Node, ID, IDToNodeN) ->
                                    orddict:store(ID, Node, IDToNodeN)
                            end, orddict:new(), NodeToID2),
    Config #config { map_node_id = NodeToID2, map_id_node = IDToNode }.

%% We also rely on the rebuilding in the above func in here. High
%% coupling, but the funcs are side by side and it keeps the code
%% simpler.
merge_node_id_maps(NodeID,
                   ConfigDest = #config { map_node_id = NodeToIDDest },
                   _ConfigSrc = #config { map_node_id = NodeToIDSrc }) ->
    NodeToIDDest1 = orddict:merge(fun (_Node, IDDest, _IDSrc) -> IDDest end,
                                  NodeToIDDest, NodeToIDSrc),
    tidy_node_id_maps(NodeID, ConfigDest #config { map_node_id = NodeToIDDest1 }).

merge_configs(NodeID, ConfigDest, ConfigSrc = #config {}) ->
    merge_node_id_maps(NodeID, ConfigDest, ConfigSrc);
merge_configs(_NodeID, Config, undefined) ->
    Config.
%% We deliberately don't have either of the other cases.

add_node_id(NewNode, NewNodeID, NodeID,
            Config = #config { map_node_id = NodeToID,
                               map_id_node = IDToNode }) ->
    {Changed, IDToNode1} =
        case orddict:find(NewNode, NodeToID) of
            error            -> {false, IDToNode};
            {ok, NewNodeID}  -> {false, IDToNode};
            {ok, NewNodeID1} -> {true,  orddict:erase(NewNodeID1, IDToNode)}
        end,
    {Changed, tidy_node_id_maps(
                NodeID, Config #config {
                          map_node_id =
                              orddict:store(NewNode, NewNodeID, NodeToID),
                          map_id_node =
                              orddict:store(NewNodeID, NewNode, IDToNode1) })}.

record_config_to_proplist(NodeID, Config = #config {}) ->
    Fields = record_info(fields, config),
    {_Pos, Proplist} =
        lists:foldl(
          fun (FieldName, {Pos, ProplistN}) ->
                  {Pos + 1, [{FieldName, element(Pos, Config)} | ProplistN]}
          end, {2, []}, Fields),
    [{node_id, NodeID} | Proplist].

%% We very deliberately completely ignore the map_* fields here or the
%% node_id. They are not semantically important from the POV of config
%% equivalence.
compare_configs(
  #config { version = V, minor_version = MV, gospel = GA, shutdown_timeout = STA, nodes = NA },
  #config { version = V, minor_version = MV, gospel = GB, shutdown_timeout = STB, nodes = NB }) ->
    case {[GA, STA, lists:usort(NA)], [GB, STB, lists:usort(NB)]} of
        {X, X} -> eq;
        _      -> invalid
    end;
compare_configs(#config { version = VA, minor_version = MVA },
                #config { version = VB, minor_version = MVB }) ->
    case {VA, MVA} > {VB, MVB} of
        true  -> gt;
        false -> lt
    end.

%% The point of this if to detect whether we really do need to join or
%% rejoin even when the cluster config has changed. Essentially, if
%% the gospel node in the new config is someone we were already
%% clustered with in the old config then we shouldn't do a wipe: we
%% are rejoining rather than joining.
%% Yes, melisma is a surprising choice. But 'compatible' or 'upgrade'
%% isn't right either. I like the idea of a cluster continuing to
%% slide from one config to another, hence melisma.
detect_melisma(#config { gospel = reset }, _OldConfig) ->
    false;
detect_melisma(#config {}, undefined) ->
    false;
detect_melisma(#config { gospel      = {node, Node},
                         map_node_id = MapNodeIDNew },
               #config { nodes       = Nodes,
                         map_node_id = MapNodeIDOld }) ->
    case [N || {N, _} <- Nodes, N =:= Node] of
        []    -> false;
        [_|_] -> case {orddict:find(Node, MapNodeIDNew),
                       orddict:find(Node, MapNodeIDOld)} of
                     {{ok, Id}, {ok, Id}} -> true;
                     _                    -> false
                 end
    end.

node_in_config(#config { nodes = Nodes }) ->
    [] =/= [N || {N, _} <- Nodes, N =:= node()].

%%----------------------------------------------------------------------------
%% Inspecting known-at-shutdown cluster state
%%----------------------------------------------------------------------------

load_last_seen_cluster_state() ->
    try {ok, rabbit_node_monitor:read_cluster_status()}
    catch {error, Err} -> {error, Err}
    end.



%%----------------------------------------------------------------------------
%% Node ID and mnesia
%%----------------------------------------------------------------------------

create_node_id() ->
    %% We can't use rabbit_guid here because it may not have been
    %% started at this stage. In reality, this isn't a massive
    %% problem: the fact we need to create a node_id implies that
    %% we're a fresh node, so the guid serial will be 0 anyway.
    erlang:md5(term_to_binary({node(), make_ref()})).

wipe_mnesia() ->
    ok = stop_mnesia(),
    ok = rabbit_mnesia:force_reset(),
    ok = ensure_start_mnesia().

stop_mnesia() ->
    case application:stop(mnesia) of
        ok                             -> ok;
        {error, {not_started, mnesia}} -> ok;
        Other                          -> Other
    end.

ensure_start_mnesia() ->
    ok = application:ensure_started(mnesia).

eliminate_mnesia_dependencies() ->
    %% rabbit_table:force_load() does not error if
    %% mnesia:force_load_table errors(!) Thus we can safely run this
    %% even in clean state - i.e. one where neither the schema nor any
    %% tables actually exist.
    ok = rabbit_table:force_load(),
    ok = rabbit_node_monitor:reset_cluster_status().

configure_cluster(Nodes) ->
    case application:load(rabbit) of
        ok                                -> ok;
        {error, {already_loaded, rabbit}} -> ok
    end,
    case Nodes of
        undefined ->
            ok = application:set_env(rabbit, cluster_nodes, {[], disc});
        [_|_] ->
            NodeNames = [N || {N, _} <- Nodes],
            Mode = proplists:get_value(node(), Nodes),
            ok = application:set_env(rabbit, cluster_nodes, {NodeNames, Mode})
    end.

stop_rabbit() ->
    case application:stop(rabbit) of
        ok                             -> ok;
        {error, {not_started, rabbit}} -> ok;
        Other                          -> Other
    end.
