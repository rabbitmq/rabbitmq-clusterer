-module(clusterer_utils).

-export([set_config/2,
         store_node/2,
         set_node_state/2,
         contains_node/2,
         make_config_active/1]).

-include("clusterer_test.hrl").

set_config(Config = #config { nodes = [_|_] },
           Test = #test { valid_config = undefined }) ->
    Test #test { config = Config, valid_config = Config };
set_config(Config = #config { nodes = [_|_], version = V },
           Test = #test { valid_config = #config { version = VV } })
  when V > VV ->
    Test #test { config = Config, valid_config = Config };
set_config(Config, Test) ->
    Test #test { config = Config }.

store_node(Node = #node { name = Name }, Test = #test { nodes = Nodes }) ->
    Test #test { nodes = orddict:store(Name, Node, Nodes) }.

set_node_state(Node = #node { name = Name, state = State }, Config) ->
    case {State, contains_node(Name, Config)} of
        {off,   _    } -> Node;
        {reset, _    } -> Node;
        {_,     true } -> Node #node { state = ready };
        {ready, false} -> Node #node { state = {pending_shutdown, 0} };
        {_,     false} -> Node %% was pending_shutdown, won't get updated
    end.

contains_node(Node,  #config { nodes = Nodes }) -> orddict:is_key(Node, Nodes);
contains_node(_Node, undefined)                 -> false.

%% Because we know that the valid config is only applied to nodes
%% which are involved in the config, modelling the propogation is
%% easy.
make_config_active(Test = #test { nodes        = Nodes,
                                  valid_config = VConfig = #config { } }) ->
    Nodes1 = orddict:map(
               fun (_Name, Node) -> set_node_state(Node, VConfig) end, Nodes),
    Test #test { nodes         = Nodes1,
                 active_config = VConfig }.
