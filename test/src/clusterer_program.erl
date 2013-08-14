-module(clusterer_program).

-export([generate_program/1]).

-include("clusterer_test.hrl").

generate_program(Test = #test {}) ->
    generate_program([], Test).

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
    %% new node.
    Step = #step { modify_node_instrs = [],
                   modify_config_instr = noop,
                   create_node_instr = noop,
                   final_state = Test },
    Step1 = step_if_seed(fun generate_modify_node_instructions/1, Step),
    Step2 = step_if_seed(fun generate_modify_config_instructions/1, Step1),
    step_if_seed(fun generate_existential_node_instructions/1, Step2).

generate_modify_node_instructions(
  Step = #step { final_state = Test = #test { nodes = Nodes } }) ->
    {NodeInstrs, Test1} =
        orddict:fold(
          fun (_Name, _Node, {_Instrs, TestN = #test { seed = 0 }}) ->
                  {[], TestN};
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
            [fun create_node_instr/1,
             case orddict:size(
                    orddict:filter(fun (_Name, #node { state = State }) ->
                                           State =:= reset orelse State =:= off
                                   end, Nodes)) of
                 0 -> [];
                 _ -> [fun delete_node_instr/1]
             end]), Test),
    step_if_seed(fun (Step1 = #step { final_state = Test2 }) ->
                         {CreateNodeInstr, Test3} = InstrFun(Test2),
                         Step1 #step { create_node_instr = CreateNodeInstr,
                                       final_state       = Test3 }
                 end, Step #step { final_state = Test1 }).

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

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
     case contains_node(Name, VConfig) andalso AConfig =/= VConfig of
         true  -> [fun start_node_with_config_instr/2];
         false -> []
     end];
modify_node_instructions(#node { name = Name, state = reset },
                         Test = #test { valid_config  = VConfig,
                                        active_config = AConfig }) ->
    [case contains_node(Name, VConfig) andalso AConfig =/= VConfig of
         true  -> [fun start_node_with_config_instr/2];
         false -> []
     end,
     case is_config_active(Test) andalso contains_node(Name, AConfig) of
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
     case contains_node(Name, VConfig) of
         true  -> [fun apply_config_instr/2];
         false -> []
     end].

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

change_shutdown_timeout_instr(
  Test = #test { config = Config = #config { shutdown_timeout = ST } }) ->
    Values = [infinity, 0, 1, 2, 10, 30],
    {Value, Test1} = choose_one([V || V <- Values, V =/= ST], Test),
    Config1 = Config #config { shutdown_timeout = Value },
    {{config_shutdown_timeout_to, Value}, set_config(Config1, Test1)}.

update_version_instr(
  Test = #test { config = Config = #config { version = V } }) ->
    Config1 = Config #config { version = V + 1 },
    {{config_version_to, V + 1}, set_config(Config1, Test)}.

change_gospel_instr(
  Test = #test { config = Config = #config { nodes  = Nodes,
                                             gospel = Gospel } }) ->
    Values = [reset | [{node, N} || N <- orddict:fetch_keys(Nodes)]],
    {Value, Test1} = choose_one([V || V <- Values, V =/= Gospel], Test),
    Config1 = Config #config { gospel = Value },
    {{config_gospel_to, Value}, set_config(Config1, Test1)}.

add_node_to_config_instr(Test = #test { config = Config =
                                            #config { nodes = ConfigNodes },
                                        nodes  = Nodes }) ->
    Values = [V || V <- orddict:fetch_keys(Nodes),
                   not orddict:is_key(V, ConfigNodes)],
    {Value, Test1} = choose_one(Values, Test),
    Config1 =
        Config #config { nodes = orddict:store(Value, disc, ConfigNodes) },
    {{config_add_node, Value}, set_config(Config1, Test1)}.

remove_node_from_config_instr(
  Test = #test { config = Config = #config { nodes  = Nodes,
                                             gospel = Gospel } }) ->
    Values = [N || N <- orddict:fetch_keys(Nodes), {node, N} =/= Gospel],
    {Value, Test1} = choose_one(Values, Test),
    Config1 = Config #config { nodes = orddict:erase(Value, Nodes) },
    {{config_remove_node, Value}, set_config(Config1, Test1)}.

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

create_node_instr(Test) ->
    {Name, Test1 = #test { nodes = Nodes }} = generate_name(Test),
    Node = #node { name  = Name,
                   pid   = undefined,
                   state = reset },
    {{create_node, Name},
     Test1 #test { nodes = orddict:store(Name, Node, Nodes) }}.

delete_node_instr(Test = #test { nodes = Nodes }) ->
    Names = orddict:fetch_keys(
              orddict:filter(fun (_Name, #node { state = State }) ->
                                     State =:= reset orelse State =:= off
                             end, Nodes)),
    {Name, Test1} = choose_one(Names, Test),
    {{delete_node, Name}, Test1 #test { nodes = orddict:erase(Name, Nodes) }}.

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

reset_node_instr(Node = #node { name = Name, state = off }, Test) ->
    {{reset_node, Name}, store_node(Node #node { state = reset }, Test)}.

start_node_instr(Node = #node { name = Name, state = State },
                 Test = #test { active_config = AConfig })
  when State =:= off orelse State =:= reset ->
    {{start_node, Name}, store_node(set_node_state(Node, AConfig), Test)}.

start_node_with_config_instr(#node { name = Name, state = State },
                             Test = #test { valid_config = VConfig })
  when State =:= off orelse State =:= reset ->
    {{start_node_with_config, Name, VConfig}, make_config_active(Test)}.

apply_config_instr(#node { name = Name },
                   Test = #test { valid_config = VConfig }) ->
    %% State = ready orelse State = {pending_shutdown, _}
    {{apply_config_to_node, Name, VConfig}, make_config_active(Test)}.

stop_node_instr(Node = #node { name = Name }, Test) ->
    {{stop_node, Name}, store_node(Node #node { state = off }, Test)}.

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

set_config(Config = #config { nodes = [_|_] },
           Test = #test { valid_config = undefined }) ->
    Test #test { config = Config, valid_config = Config };
set_config(Config = #config { nodes = [_|_], version = V },
           Test = #test { valid_config = #config { version = VV } })
  when V > VV ->
    Test #test { config = Config, valid_config = Config };
set_config(Config, Test) ->
    Test #test { config = Config }.

%% Because we know that the valid config is only applied to nodes
%% which are involved in the config, modelling the propogation is
%% easy.
make_config_active(Test = #test { nodes        = Nodes,
                                  valid_config = VConfig = #config { } }) ->
    Nodes1 = orddict:map(
               fun (_Name, Node) -> set_node_state(Node, VConfig) end, Nodes),
    Test #test { nodes         = Nodes1,
                 active_config = VConfig }.

is_config_active(#test { active_config = undefined }) ->
    false;
is_config_active(#test { nodes = Nodes,
                         active_config = #config { nodes = ConfigNodes } }) ->
    [] =/= orddict:filter(
             fun (Name, _Disc) ->
                     orddict:is_key(Name, Nodes) andalso
                         ready =:= (orddict:fetch(Name, Nodes)) #node.state
             end, ConfigNodes).

store_node(Node = #node { name = Name }, Test = #test { nodes = Nodes }) ->
    Test #test { nodes = orddict:store(Name, Node, Nodes) }.

set_node_state(Node = #node { name = Name },
               Config = #config { shutdown_timeout = ST }) ->
    case contains_node(Name, Config) of
         true  -> Node #node { state = ready };
         false -> Node #node { state = {pending_shutdown, ST} }
    end.

generate_name(Test = #test { namer = {N, Host} }) ->
    {list_to_atom(lists:flatten(io_lib:format("node~p@~s", [N, Host]))),
     Test #test { namer = {N+1, Host} }}.

noop(Test       ) -> {noop, Test}.
noop(_Node, Test) -> {noop, Test}.

noops(#test { nodes = Nodes }, Fun) ->
    lists:duplicate(1 + (orddict:size(Nodes) div 2), Fun).

choose_one_noop1(List, Test) ->
    choose_one(List ++ noops(Test, fun noop/1), Test).
choose_one_noop2(List, Test) ->
    choose_one(List ++ noops(Test, fun noop/2), Test).

choose_one(List, Test = #test { seed = Seed }) ->
    Len = length(List),
    {lists:nth(1 + (Seed rem Len), List), Test #test { seed = Seed div Len }}.

contains_node(Node,  #config { nodes = Nodes }) -> orddict:is_key(Node, Nodes);
contains_node(_Node, undefined)                 -> false.

step_if_seed(_Fun, Step = #step { final_state = #test { seed = 0 } }) ->
    Step;
step_if_seed(Fun,  Step = #step {}) ->
    Fun(Step).
