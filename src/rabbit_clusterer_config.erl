-module(rabbit_clusterer_config).

-export([load/2, load/1, store_internal/2, to_proplist/2,
         transfer_map/2, update_node_id/4, add_node_ids/3, add_node_id/4,
         compare/2, is_compatible/2,
         contains_node/2, is_singelton/2, nodenames/1, disc_nodenames/1,
         node_type/2, node_id/2, gospel/1, shutdown_timeout/1]).

-record(config, { nodes,
                  version,
                  gospel,
                  shutdown_timeout,
                  map_node_id
                }).
%%----------------------------------------------------------------------------

%% We can't put the file within mnesia dir because that upsets the
%% virgin detection in rabbit_mnesia!
internal_path() -> rabbit_mnesia:dir() ++ "-cluster.config".

external_path() -> application:get_env(rabbitmq_clusterer, config).

load(undefined)      -> load_external();
load(#config {} = C) -> case validate(C) of
                            ok  -> {ok, C};
                            Err -> Err
                        end;
load(PathOrPropList) -> load_external(PathOrPropList).

load(NodeID, Config) ->
    choose_external_or_internal(
      case load_external() of
          {ok, ExternalConfig} ->
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
                                  {ok, _NodeID, Config} -> {ok, Config};
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

choose_external_or_internal(undefined, undefined) ->
    {ok, NodeID, NewConfig} = default_config(),
    {NodeID, NewConfig, undefined};
choose_external_or_internal(NewConfig, undefined) ->
    %% We only have an external config and no internal config, so we
    %% have no NodeID, so we must generate one.
    NodeID = create_node_id(),
    {NodeID, tidy_node_id_maps(NodeID, NewConfig), undefined};
choose_external_or_internal(undefined, {NodeID, OldConfig}) ->
    {NodeID, OldConfig, OldConfig};
choose_external_or_internal(NewConfig, {NodeID, OldConfig}) ->
    case compare(NewConfig, OldConfig) of
        younger -> %% New cluster config has been applied
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

field_fold(Fun, Init) ->
    {_Pos, Res} = lists:foldl(fun (FieldName, {Pos, Acc}) ->
                                      {Pos + 1, Fun(FieldName, Pos, Acc)}
                              end, {2, Init}, record_info(fields, config)),
    Res.

to_proplist(NodeID, Config = #config {}) ->
    [{node_id, NodeID} |
     field_fold(fun (FieldName, Pos, ProplistN) ->
                        [{FieldName, element(Pos, Config)} | ProplistN]
                end, [])].

from_proplist(Proplist) when is_list(Proplist) ->
    case check_required_keys(Proplist) of
        ok ->
            Proplist1 = add_optional_keys(Proplist),
            Config = #config { nodes = Nodes } =
                field_fold(
                  fun (FieldName, Pos, ConfigN) ->
                          setelement(Pos, ConfigN,
                                     proplists:get_value(FieldName, Proplist1))
                  end, #config {}),
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
    field_fold(fun (FieldName, Pos, ok) ->
                       validate_key(FieldName, element(Pos, Config), Config);
                   (_FieldName, _Pos, {error, _E} = Err) ->
                       Err
               end, ok).

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

transfer_map(undefined, Dest) ->
    Dest;
transfer_map(#config { map_node_id = Map }, Dest = #config { }) ->
    Dest #config { map_node_id = Map }.

update_node_id(Node, #config { map_node_id = NodeToIDRemote },
               NodeID, Config = #config { map_node_id = NodeToIDLocal }) ->
    NodeToIDLocal1 = orddict:store(Node, orddict:fetch(Node, NodeToIDRemote),
                                   NodeToIDLocal),
    tidy_node_id_maps(NodeID,
                      Config #config { map_node_id = NodeToIDLocal1 }).

add_node_ids(NodeIDs, NodeID, Config = #config { map_node_id = NodeToID }) ->
    NodeToID1 = orddict:merge(fun (_Node, _A, B) -> B end,
                              NodeToID, orddict:from_list(NodeIDs)),
    tidy_node_id_maps(NodeID, Config #config { map_node_id = NodeToID1 }).

add_node_id(NewNode, NewNodeID, NodeID,
            Config = #config { map_node_id = NodeToID }) ->
    %% Note that if NewNode isn't in Config then tidy_node_id_maps
    %% will do the right thing, and also that Changed will always be
    %% false.
    Changed = case orddict:find(NewNode, NodeToID) of
                  error            -> false;
                  {ok, NewNodeID}  -> false;
                  {ok, _NewNodeID} -> true
              end,
    NodeToID1 = orddict:store(NewNode, NewNodeID, NodeToID),
    {Changed, tidy_node_id_maps(NodeID,
                                Config #config { map_node_id = NodeToID1 })}.

tidy_node_id_maps(NodeID, Config = #config { nodes = Nodes,
                                             map_node_id = NodeToID }) ->
    MyNode = node(),
    NodeToID1 = orddict:filter(fun (N, _ID) -> orddict:is_key(N, Nodes) end,
                               NodeToID),
    %% our own node_id may have changed or be missing.
    NodeToID2 = case orddict:is_key(MyNode, Nodes) of
                    true  -> orddict:store(MyNode, NodeID, NodeToID1);
                    false -> NodeToID1
                end,
    Config #config { map_node_id = NodeToID2 }.

%%----------------------------------------------------------------------------

%% We very deliberately completely ignore the map_* fields here. They
%% are not semantically important from the POV of config equivalence.
compare(#config { version = V, gospel = GA, nodes = NA, shutdown_timeout = TA },
        #config { version = V, gospel = GB, nodes = NB, shutdown_timeout = TB }) ->
    case {{GA, lists:usort(NA), TA}, {GB, lists:usort(NB), TB}} of
        {EQ, EQ} -> coeval;
        _        -> invalid
    end;
compare(#config { version = VA },
        #config { version = VB }) ->
    case VA > VB of
        true  -> younger;
        false -> older
    end.

%% If the config has changed, we need to figure out whether we need to
%% do a full join (which may well include wiping out mnesia) or
%% whether the config has simply evolved and we can do something
%% softer (maybe nothing at all). Essentially, if the gospel node in
%% the new config is someone we thought we knew but who's been reset
%% (so their node_id has changed) then we'll need to do a fresh sync
%% to them.
is_compatible(Config, Config) ->
    true;
is_compatible(#config { gospel = reset }, _OldConfig) ->
    false;
is_compatible(#config {}, undefined) ->
    false;
is_compatible(#config { gospel = {node, Node}, map_node_id = MapNodeIDNew },
              ConfigOld = #config { map_node_id = MapNodeIDOld }) ->
    case (contains_node(node(), ConfigOld) andalso
          contains_node(Node,   ConfigOld)) of
        true  -> case {orddict:find(Node, MapNodeIDNew),
                       orddict:find(Node, MapNodeIDOld)} of
                     {{ok, IdA}, {ok, IdB}} when IdA =/= IdB -> false;
                     {_        , _        }                  -> true
                 end;
        false -> false
    end.

%%----------------------------------------------------------------------------

contains_node(Node, #config { nodes = Nodes }) -> orddict:is_key(Node, Nodes).

is_singelton( Node, #config { nodes = [{Node, disc}] }) -> true;
is_singelton(_Node, _Config)                            -> false.

nodenames(#config { nodes = Nodes }) -> orddict:fetch_keys(Nodes).

disc_nodenames(#config { nodes = Nodes }) ->
    orddict:fetch_keys(orddict:filter(fun (_K, V) -> V =:= disc end, Nodes)).

node_type(Node, #config { nodes = Nodes }) -> orddict:fetch(Node, Nodes).

node_id(Node, #config { map_node_id = Map }) -> orddict:fetch(Node, Map).

gospel(#config { gospel = Gospel }) -> Gospel.

shutdown_timeout(#config { shutdown_timeout = Timeout }) -> Timeout.
