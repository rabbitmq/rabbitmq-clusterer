-module(clusterer_utils).

-export([set_config/2,
         store_node/2,
         set_node_state/2,
         contains_node/2,
         make_config_active/1,
         localise_program/2]).

-include("clusterer_test.hrl").

%%----------------------------------------------------------------------------

set_config(Config = #config { nodes = [_|_] },
           State = #state { valid_config = undefined }) ->
    State #state { config = Config, valid_config = Config };
set_config(Config = #config { nodes = [_|_], version = V },
           State = #state { valid_config = #config { version = VV } })
  when V > VV ->
    State #state { config = Config, valid_config = Config };
set_config(Config, State) ->
    State #state { config = Config }.

store_node(Node = #node { name = Name }, State = #state { nodes = Nodes }) ->
    State #state { nodes = orddict:store(Name, Node, Nodes) }.

set_node_state(Node = #node { name = Name, state = State },
               Config = #config { shutdown_timeout = ST }) ->
    case {State, contains_node(Name, Config)} of
        {off,   _    } -> Node;
        {reset, _    } -> Node;
        {_,     true } -> Node #node { state = ready };
        {ready, false} -> Node #node { state = {pending_shutdown, ST} };
        {_,     false} -> Node %% was pending_shutdown, won't get updated
    end.

contains_node(Node,  #config { nodes = Nodes }) -> orddict:is_key(Node, Nodes);
contains_node(_Node, undefined)                 -> false.

%% Because we know that the valid config is only applied to nodes
%% which are involved in the config, modelling the propogation is
%% easy.
make_config_active(State = #state { nodes        = Nodes,
                                  valid_config = VConfig = #config { } }) ->
    Nodes1 = orddict:map(
               fun (_Name, Node) -> set_node_state(Node, VConfig) end, Nodes),
    State #state { nodes         = Nodes1,
                   active_config = VConfig }.

localise_program({InitialState, Steps}, Host) ->
    {localise_state(InitialState, Host),
     [localise_step(Step, Host) || Step <- Steps]}.

localise_step(#step { modify_node_instrs     = NodeInstrs,
                      modify_config_instr    = ConfigInstr,
                      existential_node_instr = ExistentialInstr,
                      final_state            = State }, Host) ->
    #step { modify_node_instrs     = [localise_instr(Instr, Host) ||
                                         Instr <- NodeInstrs],
            modify_config_instr    = localise_instr(ConfigInstr, Host),
            existential_node_instr = localise_instr(ExistentialInstr, Host),
            final_state            = localise_state(State, Host) }.

localise_instr({Action, Name}, Host)
  when Action =:= stop_node orelse
       Action =:= start_node orelse
       Action =:= reset_node orelse
       Action =:= delete_node orelse
       Action =:= config_remove_node orelse
       Action =:= config_add_node ->
    {Action, localise_name(Name, Host)};
localise_instr({Action, Name, Config}, Host)
  when Action =:= apply_config_to_node orelse
       Action =:= start_node_with_config ->
    {Action, localise_name(Name, Host), localise_config(Config, Host)};
localise_instr({create_node, Name, Port}, Host) ->
    {create_node, localise_name(Name, Host), Port};
localise_instr({config_gospel_to, reset} = Instr, _Host) ->
    Instr;
localise_instr({config_gospel_to, {node, Name}}, Host) ->
    {config_gospel_to, {node, localise_name(Name, Host)}};
localise_instr({config_version_to, _Ver} = Instr, _Host) ->
    Instr;
localise_instr({config_shutdown_timeout_to, _ST} = Instr, _Host) ->
    Instr;
localise_instr(noop, _Host) ->
    noop.

localise_name(NodeName, Host) ->
    {Node, _Host} = rabbit_nodes:parts(NodeName),
    rabbit_nodes:make({Node, Host}).

localise_config(Config = #config { nodes = Nodes, gospel = Gospel }, Host) ->
    Config #config {
      nodes = orddict:from_list([{localise_name(Name, Host), Value} ||
                                    {Name, Value} <- orddict:to_list(Nodes)]),
      gospel = case Gospel of
                   reset        -> reset;
                   {node, Name} -> {node, localise_name(Name, Host)}
               end
     };
localise_config(undefined, _Host) ->
    undefined.

localise_state(State = #state { nodes         = Nodes,
                                config        = Config,
                                valid_config  = VConfig,
                                active_config = AConfig }, Host) ->
    State #state { nodes         =
                       orddict:from_list(
                         [{localise_name(Name, Host),
                           localise_node(Node, Host)} ||
                             {Name, Node} <- orddict:to_list(Nodes)]),
                   config        = localise_config(Config, Host),
                   valid_config  = localise_config(VConfig, Host),
                   active_config = localise_config(AConfig, Host) }.

localise_node(Node = #node { name = Name }, Host) ->
    Node #node { name = localise_name(Name, Host) }.
