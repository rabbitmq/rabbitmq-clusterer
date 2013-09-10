-module(clusterer_program).

-export([generate_program/1]).

-include("clusterer_test.hrl").

-define(BASE_PORT, 10000).

%%----------------------------------------------------------------------------

generate_program(InitialState = #state {}) ->
    {InitialState, generate_steps([], InitialState)}.

generate_steps(Steps, #state { seed = 0 }) ->
    lists:reverse(Steps);
generate_steps(Steps, State) ->
    Step = generate_step(State),
    generate_steps([Step | Steps], Step #step.final_state).

generate_step(State) ->
    %% We want to avoid any dependencies between instructions within a
    %% step - i.e. they must all be able to be exec'd in parallel. To
    %% enforce that we generate the instructions in a particular
    %% order: 1) modify an existing node; 2) modify config; 3) create
    %% new node. However, "create new node" can also include "delete
    %% node" and we need to ensure that if we delete a node it is not
    %% also used in another instruction in the same step. Thus we do
    %% the existential instruction first, but it can result in a
    %% "delayed" instruction for creation that is exec'd at the end.
    Step = #step { modify_node_instrs     = [],
                   modify_config_instr    = noop,
                   existential_node_instr = noop,
                   final_state            = State },
    Step1 = step_if_seed(fun generate_existential_node_instructions/1, Step),
    Step2 = step_if_seed(fun generate_modify_node_instructions/1, Step1),
    Step3 = step_if_seed(fun generate_modify_config_instructions/1, Step2),
    eval_delayed_existential_instruction(Step3).

generate_modify_node_instructions(
  Step = #step { final_state = State = #state { nodes = Nodes } }) ->
    {NodeInstrs, State1} =
        orddict:fold(
          fun (_Name, _Node, {Instrs, StateN = #state { seed = 0 }}) ->
                  {Instrs, StateN};
              (Name, Node = #node { name = Name }, {Instrs, StateN}) ->
                  {NodeInstrFun, StateN1} =
                      choose_one_noop2(
                        lists:flatten(
                          modify_node_instructions(Node, StateN)), StateN),
                  {NodeInstr, StateN2} = NodeInstrFun(Node, StateN1),
                  {[NodeInstr | Instrs], StateN2}
          end, {[], State}, Nodes),
    Step #step { modify_node_instrs = NodeInstrs, final_state = State1 }.

generate_modify_config_instructions(
  Step = #step { final_state = State = #state { nodes  = Nodes,
                                                config = Config } }) ->
    #config { nodes = ConfigNodes, gospel = Gospel } = Config,
    {InstrFun, State1} =
        choose_one_noop1(
          lists:flatten([fun update_version_instr/1,
                         case ConfigNodes of
                             [] -> [];
                             _  -> fun change_gospel_instr/1
                         end,
                         case orddict:size(Nodes) > orddict:size(ConfigNodes) of
                             true  -> [fun add_node_to_config_instr/1];
                             false -> []
                         end,
                         case orddict:size(ConfigNodes) > 0 andalso
                             [Gospel] =/=
                             [{node,N} || N <- orddict:fetch_keys(ConfigNodes)]
                         of
                             true  -> [fun remove_node_from_config_instr/1];
                             false -> []
                         end]), State),
    step_if_seed(fun (Step1 = #step { final_state = State2 }) ->
                         {ModifyConfigInstr, State3} = InstrFun(State2),
                         Step1 #step { modify_config_instr = ModifyConfigInstr,
                                       final_state         = State3 }
                 end, Step #step { final_state = State1 }).

generate_existential_node_instructions(
  Step = #step { final_state = State = #state { nodes = Nodes }}) ->
    {InstrFun, State1} =
        choose_one_noop1(
          lists:flatten(
            [fun create_node_fun_instr/1, %% this one is delayed
             case orddict:size( %% can only delete if we have some off nodes
                    orddict:filter(fun (_Name, #node { state = NS }) ->
                                           NS =:= reset orelse NS =:= off
                                   end, Nodes)) of
                 0 -> [];
                 _ -> [fun delete_node_instr/1]
             end]), State),
    step_if_seed(fun (Step1 = #step { final_state = State2 }) ->
                         {ExistNodeInstr, State3} = InstrFun(State2),
                         Step1 #step { existential_node_instr = ExistNodeInstr,
                                       final_state            = State3 }
                 end, Step #step { final_state = State1 }).

%%----------------------------------------------------------------------------

modify_node_instructions(#node { name = Name, state = off },
                         State = #state { valid_config  = VConfig,
                                          active_config = AConfig }) ->
    %% To keep life simpler, we only allow starting a node with the
    %% new config if the new config uses the node.
    [fun reset_node_instr/2,
     case is_config_active(State) of
         true  -> [fun start_node_instr/2];
         false -> []
     end,
     case clusterer_utils:contains_node(Name, VConfig) andalso
          AConfig =/= VConfig of
         true  -> [fun start_node_with_config_instr/2];
         false -> []
     end];
modify_node_instructions(#node { name = Name, state = reset },
                         State = #state { valid_config  = VConfig,
                                          active_config = AConfig }) ->
    [case clusterer_utils:contains_node(Name, VConfig) andalso
          AConfig =/= VConfig of
         true  -> [fun start_node_with_config_instr/2];
         false -> []
     end,
     case is_config_active(State) andalso
          clusterer_utils:contains_node(Name, AConfig) of
         true  -> [fun start_node_instr/2];
         false -> []
     end];
modify_node_instructions(#node { state = ready },
                         #state { valid_config  = VConfig,
                                  active_config = AConfig }) ->
    [fun stop_node_instr/2,
     case VConfig of
         #config {} when VConfig =/= AConfig -> [fun apply_config_instr/2];
         _                                   -> []
     end].

%%----------------------------------------------------------------------------

update_version_instr(
  State = #state { config = Config = #config { version = V } }) ->
    Config1 = Config #config { version = V + 1 },
    {{config_version_to, V + 1},
     clusterer_utils:set_config(Config1, State)}.

change_gospel_instr(
  State = #state { config = Config = #config { nodes  = Nodes,
                                             gospel = Gospel } }) ->
    Values = [reset | [{node, N} || N <- orddict:fetch_keys(Nodes)]],
    {Value, State1} = choose_one([V || V <- Values, V =/= Gospel], State),
    Config1 = Config #config { gospel = Value },
    {{config_gospel_to, Value},
     clusterer_utils:set_config(Config1, State1)}.

add_node_to_config_instr(State = #state { config = Config =
                                              #config { nodes = ConfigNodes },
                                          nodes  = Nodes }) ->
    Values = [V || V <- orddict:fetch_keys(Nodes),
                   not orddict:is_key(V, ConfigNodes)],
    {Value, State1} = choose_one(Values, State),
    Config1 =
        Config #config { nodes = orddict:store(Value, disc, ConfigNodes) },
    {{config_add_node, Value},
     clusterer_utils:set_config(Config1, State1)}.

remove_node_from_config_instr(
  State = #state { config = Config = #config { nodes  = Nodes,
                                               gospel = Gospel } }) ->
    Values = [N || N <- orddict:fetch_keys(Nodes), {node, N} =/= Gospel],
    {Value, State1} = choose_one(Values, State),
    Config1 = Config #config { nodes = orddict:erase(Value, Nodes) },
    {{config_remove_node, Value},
     clusterer_utils:set_config(Config1, State1)}.

%%----------------------------------------------------------------------------

create_node_fun_instr(State) ->
    {{delayed,
      fun (State1) ->
              {Name, Port, State2 = #state { nodes = Nodes }} =
                  generate_name_port(State1),
              Node = #node { name  = Name,
                             port  = Port,
                             pid   = undefined,
                             state = reset },
              {{create_node, Name, Port},
               State2 #state { nodes = orddict:store(Name, Node, Nodes) }}
      end}, State}.

delete_node_instr(State = #state { nodes = Nodes }) ->
    Names = orddict:fetch_keys(
              orddict:filter(fun (_Name, #node { state = NS }) ->
                                     NS =:= reset orelse NS =:= off
                             end, Nodes)),
    {Name, State1} = choose_one(Names, State),
    {{delete_node, Name}, State1 #state { nodes = orddict:erase(Name, Nodes) }}.

%%----------------------------------------------------------------------------

reset_node_instr(Node = #node { name = Name, state = off }, State) ->
    {{reset_node, Name},
     clusterer_utils:store_node(Node #node { state = reset }, State)}.

start_node_instr(Node = #node { name = Name, state = NS },
                 State = #state { active_config = AConfig })
  when NS =:= off orelse NS =:= reset ->
    {{start_node, Name},
     clusterer_utils:store_node(
       clusterer_utils:set_node_state(
         Node #node { state = ready }, AConfig), State)}.

start_node_with_config_instr(Node = #node { name = Name, state = NS },
                             State = #state { valid_config = VConfig })
  when NS =:= off orelse NS =:= reset ->
    {{start_node_with_config, Name, VConfig},
     clusterer_utils:make_config_active(
       clusterer_utils:store_node(Node #node { state = ready }, State))}.

apply_config_instr(#node { name = Name, state = ready },
                   State = #state { valid_config = VConfig }) ->
    {{apply_config_to_node, Name, VConfig},
     clusterer_utils:make_config_active(State)}.

stop_node_instr(Node = #node { name = Name }, State) ->
    {{stop_node, Name},
     clusterer_utils:store_node(Node #node { state = off }, State)}.

%%----------------------------------------------------------------------------

is_config_active(#state { active_config = undefined }) ->
    false;
is_config_active(#state { nodes = Nodes,
                          active_config = #config { nodes = ConfigNodes } }) ->
    [] =/= orddict:filter(
             fun (Name, _Disc) ->
                     orddict:is_key(Name, Nodes) andalso
                         ready =:= (orddict:fetch(Name, Nodes)) #node.state
             end, ConfigNodes).

generate_name_port(State = #state { node_count = N }) ->
    {list_to_atom(lists:flatten(io_lib:format("node~p@anyhost", [N]))),
     ?BASE_PORT + N,
     State #state { node_count = N+1 }}.

noop(State       ) -> {noop, State}.
noop(_Node, State) -> {noop, State}.

choose_one_noop1(List, State) -> choose_one([fun noop/1 | List], State).
choose_one_noop2(List, State) -> choose_one([fun noop/2 | List], State).

choose_one(List, State = #state { seed = Seed }) ->
    Len = length(List),
    {lists:nth(1 + (Seed rem Len), List), State #state { seed = Seed div Len }}.

step_if_seed(_Fun, Step = #step { final_state = #state { seed = 0 } }) ->
    Step;
step_if_seed(Fun,  Step = #step {}) ->
    Fun(Step).

%% We only need this for the create_node case so we don't overly
%% generalise this. Yes, I know this is not like me at all. Also, this
%% case definitely doesn't need further seed so we don't wrap it in
%% step_if_seed.
eval_delayed_existential_instruction(
  Step = #step { existential_node_instr = {delayed, Fun},
                 final_state            = State }) ->
    {ExistentialInstr, State1} = Fun(State),
    Step #step { existential_node_instr = ExistentialInstr,
                 final_state            = State1 };
eval_delayed_existential_instruction(Step) ->
    Step.
