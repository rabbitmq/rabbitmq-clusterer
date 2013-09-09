-module(clusterer_program).

-export([generate_program/1]).

-include("clusterer_test.hrl").

-define(BASE_PORT, 10000).

%%----------------------------------------------------------------------------

generate_program(Test = #test {}) ->
    {Test, generate_program([], Test)}.

generate_program(Program, #test { seed = 0 }) ->
    lists:reverse(Program);
generate_program(Program, Test) ->
    Step = generate_step(Test),
    generate_program([Step | Program], Step #step.final_state).

generate_step(Test) ->
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
                   final_state            = Test },
    Step1 = step_if_seed(fun generate_existential_node_instructions/1, Step),
    Step2 = step_if_seed(fun generate_modify_node_instructions/1, Step1),
    Step3 = step_if_seed(fun generate_modify_config_instructions/1, Step2),
    eval_delayed_existential_instruction(Step3).

generate_modify_node_instructions(
  Step = #step { final_state = Test = #test { nodes = Nodes } }) ->
    {NodeInstrs, Test1} =
        orddict:fold(
          fun (_Name, _Node, {Instrs, TestN = #test { seed = 0 }}) ->
                  {Instrs, TestN};
              (Name, Node = #node { name = Name }, {Instrs, TestN}) ->
                  {NodeInstrFun, TestN1} =
                      choose_one_noop2(
                        lists:flatten(
                          modify_node_instructions(Node, TestN)), TestN),
                  {NodeInstr, TestN2} = NodeInstrFun(Node, TestN1),
                  {[NodeInstr | Instrs], TestN2}
          end, {[], Test}, Nodes),
    Step #step { modify_node_instrs = NodeInstrs, final_state = Test1 }.

generate_modify_config_instructions(
  Step = #step { final_state = Test = #test { nodes  = Nodes,
                                              config = Config } }) ->
    #config { nodes = ConfigNodes, gospel = Gospel } = Config,
    {InstrFun, Test1} =
        choose_one_noop1(
          lists:flatten([fun change_shutdown_timeout_instr/1,
                         fun update_version_instr/1,
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
                         end]), Test),
    step_if_seed(fun (Step1 = #step { final_state = Test2 }) ->
                         {ModifyConfigInstr, Test3} = InstrFun(Test2),
                         Step1 #step { modify_config_instr = ModifyConfigInstr,
                                       final_state         = Test3 }
                 end, Step #step { final_state = Test1 }).

generate_existential_node_instructions(
  Step = #step { final_state = Test = #test { nodes = Nodes }}) ->
    {InstrFun, Test1} =
        choose_one_noop1(
          lists:flatten(
            [fun create_node_fun_instr/1, %% this one is delayed
             case orddict:size(
                    orddict:filter(fun (_Name, #node { state = State }) ->
                                           State =:= reset orelse State =:= off
                                   end, Nodes)) of
                 0 -> [];
                 _ -> [fun delete_node_instr/1]
             end]), Test),
    step_if_seed(fun (Step1 = #step { final_state = Test2 }) ->
                         {ExistNodeInstr, Test3} = InstrFun(Test2),
                         Step1 #step { existential_node_instr = ExistNodeInstr,
                                       final_state            = Test3 }
                 end, Step #step { final_state = Test1 }).

%%----------------------------------------------------------------------------

modify_node_instructions(#node { name = Name, state = off },
                         Test = #test { valid_config  = VConfig,
                                        active_config = AConfig }) ->
    %% To keep life simpler, we only allow starting a node with the
    %% new config if the new config uses the node. If the config
    %% didn't, then yes, we could model that that node would go into
    %% pending_shutdown (and the new config wouldn't become active
    %% across the cluster), but it would then be very complex to model
    %% what would happen if other nodes contacted the node when in
    %% pending_shutdown - there might well be ways in which this
    %% config could be sent to other nodes and thus become active.
    [fun reset_node_instr/2,
     case is_config_active(Test) of
         true  -> [fun start_node_instr/2];
         false -> []
     end,
     case clusterer_utils:contains_node(Name, VConfig) andalso
          AConfig =/= VConfig of
         true  -> [fun start_node_with_config_instr/2];
         false -> []
     end];
modify_node_instructions(#node { name = Name, state = reset },
                         Test = #test { valid_config  = VConfig,
                                        active_config = AConfig }) ->
    [case clusterer_utils:contains_node(Name, VConfig) andalso
          AConfig =/= VConfig of
         true  -> [fun start_node_with_config_instr/2];
         false -> []
     end,
     case is_config_active(Test) andalso
          clusterer_utils:contains_node(Name, AConfig) of
         true  -> [fun start_node_instr/2];
         false -> []
     end];
modify_node_instructions(#node { state = ready },
                         #test { valid_config  = VConfig,
                                 active_config = AConfig }) ->
    [fun stop_node_instr/2,
     case VConfig of
         #config {} when VConfig =/= AConfig -> [fun apply_config_instr/2];
         _                                   -> []
     end];
modify_node_instructions(#node { name = Name, state = {pending_shutdown, _} },
                         #test { valid_config = VConfig }) ->
    %% As with state=off, we only allow apply_config_instr if the node
    %% is involved in the config. By definition, if Node is
    %% pending_shutdown and VConfig contains Node then VConfig =/=
    %% AConfig.
    [fun stop_node_instr/2,
     case clusterer_utils:contains_node(Name, VConfig) of
         true  -> [fun apply_config_instr/2];
         false -> []
     end].

%%----------------------------------------------------------------------------

change_shutdown_timeout_instr(
  Test = #test { config = Config = #config { shutdown_timeout = ST } }) ->
    Values = [infinity, 0, 1, 10],
    {Value, Test1} = choose_one([V || V <- Values, V =/= ST], Test),
    Config1 = Config #config { shutdown_timeout = Value },
    {{config_shutdown_timeout_to, Value},
     clusterer_utils:set_config(Config1, Test1)}.

update_version_instr(
  Test = #test { config = Config = #config { version = V } }) ->
    Config1 = Config #config { version = V + 1 },
    {{config_version_to, V + 1},
     clusterer_utils:set_config(Config1, Test)}.

change_gospel_instr(
  Test = #test { config = Config = #config { nodes  = Nodes,
                                             gospel = Gospel } }) ->
    Values = [reset | [{node, N} || N <- orddict:fetch_keys(Nodes)]],
    {Value, Test1} = choose_one([V || V <- Values, V =/= Gospel], Test),
    Config1 = Config #config { gospel = Value },
    {{config_gospel_to, Value},
     clusterer_utils:set_config(Config1, Test1)}.

add_node_to_config_instr(Test = #test { config = Config =
                                            #config { nodes = ConfigNodes },
                                        nodes  = Nodes }) ->
    Values = [V || V <- orddict:fetch_keys(Nodes),
                   not orddict:is_key(V, ConfigNodes)],
    {Value, Test1} = choose_one(Values, Test),
    Config1 =
        Config #config { nodes = orddict:store(Value, disc, ConfigNodes) },
    {{config_add_node, Value},
     clusterer_utils:set_config(Config1, Test1)}.

remove_node_from_config_instr(
  Test = #test { config = Config = #config { nodes  = Nodes,
                                             gospel = Gospel } }) ->
    Values = [N || N <- orddict:fetch_keys(Nodes), {node, N} =/= Gospel],
    {Value, Test1} = choose_one(Values, Test),
    Config1 = Config #config { nodes = orddict:erase(Value, Nodes) },
    {{config_remove_node, Value},
     clusterer_utils:set_config(Config1, Test1)}.

%%----------------------------------------------------------------------------

create_node_fun_instr(Test) ->
    {{delayed,
      fun (Test1) ->
              {Name, Port, Test2 = #test { nodes = Nodes }} =
                  generate_name_port(Test1),
              Node = #node { name  = Name,
                             port  = Port,
                             pid   = undefined,
                             state = reset },
              {{create_node, Name, Port},
               Test2 #test { nodes = orddict:store(Name, Node, Nodes) }}
      end}, Test}.

delete_node_instr(Test = #test { nodes = Nodes }) ->
    Names = orddict:fetch_keys(
              orddict:filter(fun (_Name, #node { state = State }) ->
                                     State =:= reset orelse State =:= off
                             end, Nodes)),
    {Name, Test1} = choose_one(Names, Test),
    {{delete_node, Name}, Test1 #test { nodes = orddict:erase(Name, Nodes) }}.

%%----------------------------------------------------------------------------

reset_node_instr(Node = #node { name = Name, state = off }, Test) ->
    {{reset_node, Name},
     clusterer_utils:store_node(Node #node { state = reset }, Test)}.

start_node_instr(Node = #node { name = Name, state = State },
                 Test = #test { active_config = AConfig })
  when State =:= off orelse State =:= reset ->
    {{start_node, Name},
     clusterer_utils:store_node(
       clusterer_utils:set_node_state(
         Node #node { state = ready }, AConfig), Test)}.

start_node_with_config_instr(Node = #node { name = Name, state = State },
                             Test = #test { valid_config = VConfig })
  when State =:= off orelse State =:= reset ->
    {{start_node_with_config, Name, VConfig},
     clusterer_utils:make_config_active(
       clusterer_utils:store_node(Node #node { state = ready }, Test))}.

apply_config_instr(#node { name = Name },
                   Test = #test { valid_config = VConfig }) ->
    %% State = ready orelse State = {pending_shutdown, _}
    {{apply_config_to_node, Name, VConfig},
     clusterer_utils:make_config_active(Test)}.

stop_node_instr(Node = #node { name = Name }, Test) ->
    {{stop_node, Name},
     clusterer_utils:store_node(Node #node { state = off }, Test)}.

%%----------------------------------------------------------------------------

is_config_active(#test { active_config = undefined }) ->
    false;
is_config_active(#test { nodes = Nodes,
                         active_config = #config { nodes = ConfigNodes } }) ->
    [] =/= orddict:filter(
             fun (Name, _Disc) ->
                     orddict:is_key(Name, Nodes) andalso
                         ready =:= (orddict:fetch(Name, Nodes)) #node.state
             end, ConfigNodes).

generate_name_port(Test = #test { node_count = N }) ->
    {list_to_atom(lists:flatten(io_lib:format("node~p@anyhost", [N]))),
     ?BASE_PORT + N,
     Test #test { node_count = N+1 }}.

noop(Test       ) -> {noop, Test}.
noop(_Node, Test) -> {noop, Test}.

choose_one_noop1(List, Test) -> choose_one([fun noop/1 | List], Test).
choose_one_noop2(List, Test) -> choose_one([fun noop/2 | List], Test).

choose_one(List, Test = #test { seed = Seed }) ->
    Len = length(List),
    {lists:nth(1 + (Seed rem Len), List), Test #test { seed = Seed div Len }}.

step_if_seed(_Fun, Step = #step { final_state = #test { seed = 0 } }) ->
    Step;
step_if_seed(Fun,  Step = #step {}) ->
    Fun(Step).

%% We only need this for the create_node case so we don't overly
%% generalise this. Yes, I know this is not like me at all. Also, this
%% case definitely doesn't need further seed so we don't wrap it in
%% step_if_seed.
eval_delayed_existential_instruction(
  Step = #step { existential_node_instr = {delayed, Fun},
                 final_state            = Test }) ->
    {ExistentialInstr, Test1} = Fun(Test),
    Step #step { existential_node_instr = ExistentialInstr,
                 final_state            = Test1 };
eval_delayed_existential_instruction(Step) ->
    Step.
