-module(rabbit_clusterer_utils).

-include("rabbit_clusterer.hrl").

-export([default_config/1,
         proplist_config_to_record/1,
         record_config_to_proplist/1,
         load_last_seen_cluster_state/0,
         compare_configs/2,
         ensure_node_id/0,
         wipe_mnesia/1,
         merge_node_id_maps/2,
         add_node_id/3]).

default_config(NodeID) ->
    proplist_config_to_record(
      [{nodes, [{node(), disc}]},
       {version, 0},
       {minor_version, 0},
       {gospel, {node, node()}},
       {shutdown_timeout, infinity},
       {map_id_node, orddict:from_list([{NodeID, node()}])},
       {map_node_id, orddict:from_list([{node(), NodeID}])}
      ]).

required_keys() ->
    [nodes, version, gospel, shutdown_timeout].

optional_keys() ->
    [{minor_version, 0},
     {map_node_id, orddict:new()},
     {map_id_node, orddict:new()}].

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
    Config2 = #config { gospel = Gospel } =
        tidy_node_id_maps(Config1 #config { nodes = Nodes1 }),
    case Gospel of
        reset        -> Config2;
        {node, Node} -> true = proplists:is_defined(Node, Nodes1), %% ASSERTION
                        Config2
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
tidy_node_id_maps(Config = #config { nodes = Nodes,
                             map_node_id = NodeToID }) ->
    NodeNames = [N || {N, _} <- Nodes],
    NodesToRemove = orddict:fetch_keys(NodeToID) -- NodeNames,
    NodeToID1 = lists:foldr(fun orddict:erase/2, NodeToID, NodesToRemove),
    IDToNode = orddict:fold(fun (Node, ID, IDToNodeN) ->
                                    orddict:store(ID, Node, IDToNodeN)
                            end, orddict:new(), NodeToID1),
    Config #config { map_node_id = NodeToID1, map_id_node = IDToNode }.

%% We also rely on the rebuilding in the above func in here. High
%% coupling, but the funcs are side by side and it keeps the code
%% simpler.
merge_node_id_maps(ConfigDest = #config { map_node_id = NodeToIDDest },
           _ConfigSrc = #config { map_node_id = NodeToIDSrc }) ->
    NodeToIDDest1 = orddict:merge(
                      fun (_Node, IDDest, _IDSrc) -> IDDest end,
                      NodeToIDDest, NodeToIDSrc),
    tidy_node_id_maps(ConfigDest #config { map_node_id = NodeToIDDest1 }).

add_node_id(Node, NodeID, Config = #config { map_node_id = NodeToID,
                                             map_id_node = IDToNode }) ->
    Changed = case orddict:find(Node, NodeToID) of
                  error         -> false;
                  {ok,  NodeID} -> false;
                  {ok, _NodeID} -> true
              end,
    {Changed,
     tidy_node_id_maps(
       Config #config { map_node_id = orddict:store(Node, NodeID, NodeToID),
                        map_id_node = orddict:store(NodeID, Node, IDToNode) })}.

record_config_to_proplist(Config = #config {}) ->
    Fields = record_info(fields, config),
    {_Pos, Proplist} =
        lists:foldl(
          fun (FieldName, {Pos, ProplistN}) ->
                  {Pos + 1, [{FieldName, element(Pos, Config)} | ProplistN]}
          end, {2, []}, Fields),
    Proplist.

%% We very deliberately completely ignore the map_* fields here. They
%% are not semantically important from the pov of config equivalence.
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

node_id_file_path() ->
    filename:join(rabbit_mnesia:dir(), "node_id").

ensure_node_id() ->
    case rabbit_file:read_term_file(node_id_file_path()) of
        {ok, [NodeId]}    -> {ok, NodeId};
        {error, enoent}   -> create_node_id();
        {error, _E} = Err -> Err
    end.

create_node_id() ->
    %% We can't use rabbit_guid here because it hasn't been started at
    %% this stage. In reality, this isn't a massive problem: the fact
    %% we need to create a node_id implies that we're a fresh node, so
    %% the guid serial will be 0 anyway.
    NodeId = erlang:md5(term_to_binary({node(), make_ref()})),
    ok = write_node_id(NodeId),
    {ok, NodeId}.

write_node_id(NodeID) ->
    case rabbit_file:write_term_file(node_id_file_path(), [NodeID]) of
        ok                -> ok;
        {error, _E} = Err -> Err
    end.

wipe_mnesia(NodeID) ->
    ok = rabbit_mnesia:force_reset(),
    ok = write_node_id(NodeID).
