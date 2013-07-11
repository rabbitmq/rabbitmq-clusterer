-module(rabbit_clusterer_utils).

-include("rabbit_clusterer.hrl").

-export([default_config/0,
         proplist_config_to_record/1,
         record_config_to_proplist/1,
         load_last_seen_cluster_state/0,
         compare_configs/2,
         ensure_node_id/0,
         wipe_mnesia/1]).

default_config() ->
    proplist_config_to_record(
      [{nodes, [{node(), disc}]},
       {version, 0},
       {minor_version, 0},
       {gospel, {node, node()}},
       {shutdown_timeout, infinity}]).

required_keys() ->
    [nodes, version, gospel, shutdown_timeout].

optional_keys() ->
    [{minor_version, 0}].

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
    Config1 #config { nodes = normalise_nodes(Nodes) }.

ensure_entries(Entries, Proplist) ->
    lists:foldl(fun ({Key, _Default} = E, ProplistN) ->
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

record_config_to_proplist(Config = #config {}) ->
    Fields = record_info(fields, config),
    {_Pos, Proplist} =
        lists:foldl(
          fun (FieldName, {Pos, ProplistN}) ->
                  {Pos + 1, [{FieldName, element(Pos, Config)} | ProplistN]}
          end, {2, []}, Fields),
    Proplist.

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
