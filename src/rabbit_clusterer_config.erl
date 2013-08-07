-module(rabbit_clusterer_config).

-include("rabbit_clusterer.hrl").

-export([load/2, load/1, store_internal/2, to_proplist/2,
         merge/3, add_node_id/4, compare/2, detect_melisma/2,
         contains_node/2, disc_nodenames/1, nodenames/1, categorise/3]).

%%----------------------------------------------------------------------------

%% We can't put the file within mnesia dir because that upsets the
%% virgin detection in rabbit_mnesia!
internal_path() -> rabbit_mnesia:dir() ++ "-cluster.config".

external_path() -> application:get_env(rabbitmq_clusterer, config).

load(undefined)      -> load_external();
load(#config {} = C) -> case validate(C) of
                            ok  -> C;
                            Err -> Err
                        end;
load(PathOrPropList) -> load_external(PathOrPropList).

load(NodeID, Config) ->
    choose_external_or_internal(
      case load_external() of
          ExternalConfig = #config {} ->
              ExternalConfig;
          {error, no_external_config_provided} ->
              undefined;
          {error, Error} ->
              error_logger:info_msg(
                "Ignoring external configuration due to error: ~p~n", [Error]),
              undefined
      end,
      case Config of
          undefined -> load_internal();
          _         -> {NodeID, Config}
      end).

load_external() ->
    case external_path() of
        {ok, PathOrProplist} -> load_external(PathOrProplist);
        undefined            -> {error, no_external_config_provided}
    end.

load_external(PathOrProplist) when is_list(PathOrProplist) ->
    ProplistOrErr = case PathOrProplist of
                        [{_,_}|_] -> {ok, [PathOrProplist]};
                        [_|_]     -> rabbit_file:read_term_file(PathOrProplist)
                    end,
    case ProplistOrErr of
        {ok, [Proplist]}   -> case from_proplist(Proplist) of
                                  {ok, _NodeID, Config} -> Config;
                                  {error, _} = Error    -> Error
                              end;
        {ok, Terms}        -> {error, rabbit_misc:format(
                                        "Config is not a single term: ~p",
                                        [Terms])};
        {error, _} = Error -> Error
    end;
load_external(Other) ->
    {error, rabbit_misc:format("External config not a path or proplist: ~p",
                               [Other])}.

load_internal() ->
    Proplist = case rabbit_file:read_term_file(internal_path()) of
                   {error, enoent}               -> undefined;
                   {ok, [Proplist1 = [{_,_}|_]]} -> Proplist1
               end,
    case Proplist of
        undefined -> undefined;
        _         -> {ok, NodeID, Config} = from_proplist(Proplist),
                     true = is_binary(NodeID), %% ASSERTION
                     {NodeID, Config}
    end.

store_internal(NodeID, Config) ->
    ok = rabbit_file:write_term_file(internal_path(),
                                     [to_proplist(NodeID, Config)]).

%%----------------------------------------------------------------------------

choose_external_or_internal(undefined, undefined) ->
    {ok, NodeID, NewConfig} = default_config(),
    {NodeID, NewConfig, undefined};
choose_external_or_internal(NewConfig, undefined) ->
    %% We only have an external config and no internal config, so we
    %% have no NodeID, so we must generate one.
    NodeID = create_node_id(),
    NewConfig1 = merge(NodeID, NewConfig, undefined),
    {NodeID, NewConfig1, undefined};
choose_external_or_internal(undefined, {NodeID, OldConfig}) ->
    {NodeID, OldConfig, OldConfig};
choose_external_or_internal(NewConfig, {NodeID, OldConfig}) ->
    case compare(NewConfig, OldConfig) of
        gt      -> %% New cluster config has been applied
                   {NodeID, NewConfig, OldConfig};
        invalid -> error_logger:info_msg(
                     "Ignoring invalid user-provided configuration", []),
                   {NodeID, OldConfig, OldConfig};
        _       -> %% All other cases, we ignore the user-provided config.
                   {NodeID, OldConfig, OldConfig}
    end.

%% Note that here we intentionally deal with NodeID being in the
%% proplist as on disk but not in the #config record.
default_config() ->
    NodeID = create_node_id(),
    MyNode = node(),
    from_proplist(
      [{nodes,            [{MyNode, disc}]},
       {version,          0},
       {gospel,           {node, MyNode}},
       {shutdown_timeout, infinity},
       {node_id,          NodeID},
       {map_node_id,      orddict:from_list([{MyNode, NodeID}])}
      ]).

create_node_id() ->
    %% We can't use rabbit_guid here because it may not have been
    %% started at this stage. We only need a fresh node_id when we're
    %% a virgin node. But we also want to ensure that when we are a
    %% virgin node our node id will be different from if we existed
    %% previously, hence the use of now() which can go wrong if time
    %% is set backwards, but we hope that won't happen.
    erlang:md5(term_to_binary({node(), now()})).

%%----------------------------------------------------------------------------

required_keys() -> [nodes, version, gospel, shutdown_timeout].

optional_keys() -> [{map_node_id, orddict:new()}].

to_proplist(NodeID, Config = #config {}) ->
    Fields = record_info(fields, config),
    {_Pos, Proplist} =
        lists:foldl(
          fun (FieldName, {Pos, ProplistN}) ->
                  {Pos + 1, [{FieldName, element(Pos, Config)} | ProplistN]}
          end, {2, []}, Fields),
    [{node_id, NodeID} | Proplist].

from_proplist(Proplist) when is_list(Proplist) ->
    case check_required_keys(Proplist) of
        ok ->
            Proplist1 = add_optional_keys(Proplist),
            Fields = record_info(fields, config),
            {_Pos, Config = #config { nodes = Nodes }} =
                lists:foldl(
                  fun (FieldName, {Pos, ConfigN}) ->
                          Value = proplists:get_value(FieldName, Proplist1),
                          {Pos + 1, setelement(Pos, ConfigN, Value)}
                  end, {2, #config {}}, Fields),
            case validate(Config) of
                ok ->
                    {ok, proplists:get_value(node_id, Proplist1),
                     Config #config { nodes = normalise_nodes(Nodes) }};
                {error, _} = Err ->
                    Err
            end;
        {error, _} = Err ->
            Err
    end;
from_proplist(Other) ->
    {error, rabbit_misc:format("Config is not a proplist: ~p", [Other])}.

check_required_keys(Proplist) ->
    case required_keys() -- proplists:get_keys(Proplist) of
        []      -> ok;
        Missing -> {error, rabbit_misc:format(
                             "Required keys missing from cluster config: ~p",
                             [Missing])}
    end.

add_optional_keys(Proplist) ->
    lists:foldr(fun ({Key, _Default} = E, ProplistN) ->
                        case proplists:is_defined(Key, ProplistN) of
                            true  -> ProplistN;
                            false -> [E | ProplistN]
                        end
                end, Proplist, optional_keys()).

validate(Config) ->
    {Result, _Pos} =
        lists:foldl(
          fun (FieldName, {ok, Pos}) ->
                  {validate_key(FieldName, element(Pos, Config), Config),
                   Pos+1};
              (_FieldName, {{error, _E}, _Pos} = Err) ->
                  Err
          end, {ok, 2}, record_info(fields, config)),
    Result.

validate_key(version, Version, _Config)
  when is_integer(Version) andalso Version >= 0 ->
    ok;
validate_key(version, Version, _Config) ->
    {error, rabbit_misc:format("Require version to be non-negative integer: ~p",
                               [Version])};
validate_key(gospel, reset, _Config) ->
    ok;
validate_key(gospel, {node, Node}, Config = #config { nodes = Nodes }) ->
    case [true || N <- Nodes,
                  Node =:= N orelse
                  {Node, disc} =:= N orelse
                  {Node, disk} =:= N] of
        []    -> {error, rabbit_misc:format(
                           "Node in gospel (~p) is not in nodes (~p)",
                           [Node, Config #config.nodes])};
        [_|_] -> ok
    end;
validate_key(gospel, Gospel, _Config) ->
    {error, rabbit_misc:format("Invalid gospel setting: ~p", [Gospel])};
validate_key(shutdown_timeout, infinity, _Config) ->
    ok;
validate_key(shutdown_timeout, Timeout, _Config)
  when is_integer(Timeout) andalso Timeout >= 0 ->
    ok;
validate_key(shutdown_timeout, Timeout, _Config) ->
    {error,
     rabbit_misc:format(
       "Require shutdown_timeout to be 'infinity' or non-negative integer: ~p",
       [Timeout])};
validate_key(nodes, Nodes, _Config) when is_list(Nodes) ->
    {Result, Disc, NodeNames} =
        lists:foldr(
          fun ({Node, disc}, {ok, _, NN}) when is_atom(Node) ->
                  {ok, true, [Node | NN]};
              ({Node, disk}, {ok, _, NN}) when is_atom(Node) ->
                  {ok, true, [Node | NN]};
              ({Node, ram }, {ok, D, NN}) when is_atom(Node) ->
                  {ok, D,    [Node | NN]};
              (Node,         {ok, _, NN}) when is_atom(Node) ->
                  {ok, true, [Node | NN]};
              (Other,        {ok, _, _NN}) ->
                  {error, rabbit_misc:format("Invalid node: ~p", [Other]), []};
              (_, {error, _E, _NN} = Err) -> Err
          end, {ok, false, []}, Nodes),
    case {Result, Disc, length(NodeNames) =:= length(lists:usort(NodeNames))} of
        {ok, true, true} ->
            ok;
        {ok, true, false} ->
            {error, rabbit_misc:format(
                      "Some nodes specified more than once: ~p", [NodeNames])};
        {ok, false, _} when length(NodeNames) =:= 0 ->
            ok;
        {ok, false, _} ->
            {error, rabbit_misc:format(
                      "Require at least one disc node: ~p", [Nodes])};
        {error, Err, _} ->
            {error, Err}
    end;
validate_key(nodes, Nodes, _Config) ->
    {error,
     rabbit_misc:format("Require nodes to be a list of nodes: ~p", [Nodes])};
validate_key(map_node_id, Orddict, _Config) when is_list(Orddict) ->
    ok;
validate_key(map_node_id, Orddict, _Config) ->
    {error,
     rabbit_misc:format("Requires map_node_id to be an orddict: ~p", [Orddict])}.

normalise_nodes(Nodes) when is_list(Nodes) ->
    orddict:from_list(
      lists:usort(
        lists:map(fun ({Node, disc} = E) when is_atom(Node) -> E;
                      ({Node, disk})     when is_atom(Node) -> {Node, disc};
                      (Node)             when is_atom(Node) -> {Node, disc};
                      ({Node, ram} = E)  when is_atom(Node) -> E
                  end, Nodes))).

%%----------------------------------------------------------------------------

merge(NodeID,
      ConfigDest = #config { map_node_id = NodeToIDDest },
      _ConfigSrc = #config { map_node_id = NodeToIDSrc }) ->
    NodeToIDDest1 = orddict:merge(fun (_Node, IDDest, _IDSrc) -> IDDest end,
                                  NodeToIDDest, NodeToIDSrc),
    tidy_node_id_maps(NodeID,
                      ConfigDest #config { map_node_id = NodeToIDDest1 });
merge(NodeID, Config, undefined) ->
    tidy_node_id_maps(NodeID, Config).
%% We deliberately don't have either of the other cases.

tidy_node_id_maps(NodeID, Config = #config { nodes = Nodes,
                                             map_node_id = NodeToID }) ->
    %% We always remove ourself from the maps to take into account our
    %% own node_id may have changed (and then add ourself back in).
    MyNode = node(),
    NodeNames = orddict:fetch_keys(Nodes) -- [MyNode],
    NodesToRemove = orddict:fetch_keys(NodeToID) -- NodeNames,
    NodeToID1 = lists:foldr(fun orddict:erase/2, NodeToID, NodesToRemove),
    %% Add ourselves in. In addition to the above, consider that we
    %% could be new to the cluster and so there was never a mapping
    %% for us anyway.
    NodeToID2 = case orddict:is_key(MyNode, Nodes) of
                    true  -> orddict:store(MyNode, NodeID, NodeToID1);
                    false -> NodeToID1
                end,
    Config #config { map_node_id = NodeToID2 }.

%%----------------------------------------------------------------------------

add_node_id(NewNode, NewNodeID, NodeID,
            Config = #config { map_node_id = NodeToID }) ->
    %% Note that if NewNode isn't in Config then tidy_node_id_maps
    %% will do the right thing, and also that Changed will always be
    %% false.
    Changed =
        case orddict:find(NewNode, NodeToID) of
            error            -> false;
            {ok, NewNodeID}  -> false;
            {ok, _NewNodeID} -> true
        end,
    {Changed, tidy_node_id_maps(
                NodeID, Config #config {
                          map_node_id =
                              orddict:store(NewNode, NewNodeID, NodeToID) })}.

%% We very deliberately completely ignore the map_* fields here. They
%% are not semantically important from the POV of config equivalence.
compare(#config { version = V, gospel = GA, nodes = NA,
                  shutdown_timeout = STA },
        #config { version = V, gospel = GB, nodes = NB,
                  shutdown_timeout = STB }) ->
    case {[GA, STA, lists:usort(NA)], [GB, STB, lists:usort(NB)]} of
        {EQ, EQ} -> eq;
        _        -> invalid
    end;
compare(#config { version = VA },
        #config { version = VB }) ->
    case VA > VB of
        true  -> gt;
        false -> lt
    end.

%% If the config has changed, we need to figure out whether we need to
%% do a full join (which may well include wiping out mnesia) or
%% whether the config has simply evolved and we can do something
%% softer (maybe nothing at all). Essentially, if the gospel node in
%% the new config is someone we thought we knew but who's been reset
%% (so their node_id has changed) then we'll need to do a fresh sync
%% to them.
%% Yes, melisma is a surprising choice. But 'compatible' or 'upgrade'
%% isn't right either. I like the idea of a cluster continuing to
%% slide from one config to another, hence melisma.
detect_melisma(Config, Config) ->
    true;
detect_melisma(#config { gospel = reset }, _OldConfig) ->
    false;
detect_melisma(#config {}, undefined) ->
    false;
detect_melisma(#config { gospel = {node, Node}, map_node_id = MapNodeIDNew },
               ConfigOld = #config { map_node_id = MapNodeIDOld }) ->
    case contains_node(node(), ConfigOld) of
        true ->
            case contains_node(Node, ConfigOld) of
                true  -> case {orddict:find(Node, MapNodeIDNew),
                               orddict:find(Node, MapNodeIDOld)} of
                             {{ok, IdA}, {ok, IdB}} when IdA =/= IdB -> false;
                             {_        , _        }                  -> true
                         end;
                false -> false
            end;
        false ->
            false
    end.

contains_node(Node, #config { nodes = Nodes }) ->
    orddict:is_key(Node, Nodes).

nodenames(#config { nodes = Nodes }) ->
    orddict:fetch_keys(Nodes).

disc_nodenames(#config { nodes = Nodes }) ->
    orddict:fetch_keys(orddict:filter(fun (_K, V) -> V =:= disc end, Nodes)).

categorise(NodeConfigList, Config, NodeID) ->
    lists:foldr(
      fun (_Relpy, {YoungestN, OlderThanUsN, _StatusDictN} = Acc)
            when YoungestN =:= invalid orelse OlderThanUsN =:= invalid ->
              Acc;
          ({N, preboot}, {YoungestN, OlderThanUsN, StatusDictN}) ->
              {YoungestN, OlderThanUsN, dict:append(preboot, N, StatusDictN)};
          ({N, {ConfigN, StatusN}}, {YoungestN, OlderThanUsN, StatusDictN}) ->
              {case compare(ConfigN, YoungestN) of
                   invalid -> invalid;
                   lt      -> YoungestN;
                   _       -> %% i.e. gt *or* eq - must merge if eq too!
                              merge(NodeID, ConfigN, YoungestN)
               end,
               case compare(ConfigN, Config) of
                   invalid -> invalid;
                   lt      -> [N | OlderThanUsN];
                   _       -> OlderThanUsN
               end,
               dict:append(StatusN, N, StatusDictN)}
      end, {Config, [], dict:new()}, NodeConfigList).
