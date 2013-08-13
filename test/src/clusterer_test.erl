-module(clusterer_test).

-export([test/0, test/1]).

-record(test, { seed,
                namer,
                nodes,
                config
              }).

-record(node, { name,
                pid,
                state
              }).

-record(config, { version,
                  nodes,
                  gospel,
                  shutdown_timeout }).

-record(step, { modify_node_instrs,
                modify_config_instr,
                create_node_instr,
                final_state }).

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

generate_program(Test) ->
    generate_program([], Test).

generate_program(Program, #test { seed = 0 }) ->
    lists:reverse(Program);
generate_program(Program, Test) ->
    {Step, Test1} = generate_step(Test),
    generate_program([Step | Program], Test1).

generate_step(Test = #test { nodes = Nodes, config = Config }) ->
    %% We want to avoid any dependencies between instructions within a
    %% step - i.e. they must all be able to be exec'd in parallel. To
    %% enforce that we generate the instructions in a particular
    %% order: 1) modify an existing node; 2) modify config; 3) create
    %% new node.
    {NodeInstrs, Test1} =
        orddict:fold(
          fun (Name, Node = #node { name = Name },
               {Instrs, TestN = #test { nodes = NodesN }}) ->
                  {NodeInstrFun, TestN1} = choose_one_noop2(modify_node_instructions(Node, TestN), TestN),
                  {NodeInstr, Node1} = NodeInstrFun(Node, Config),
                  {[NodeInstr | Instrs], TestN1 #test { nodes = orddict:store(Name, Node1, NodesN) }}
          end, {[], Test}, Nodes),
    {ModifyConfigInstrFun, Test2} = choose_one_noop(modify_config_instructions(Test1), Test1),
    {ModifyConfigInstr, Test3} = ModifyConfigInstrFun(Test2),
    {CreateNodeInstrFun, Test4} = choose_one_noop(create_node_instructions(), Test3),
    {CreateNodeInstr, Test5} = CreateNodeInstrFun(Test4),
    {#step { modify_node_instrs  = NodeInstrs,
             modify_config_instr = ModifyConfigInstr,
             create_node_instr   = CreateNodeInstr,
             final_state         = Test5 }, Test5}.

modify_config_instructions(#test { config = Config =
                                       #config { nodes = ConfigNodes },
                                   nodes  = Nodes }) ->
    lists:flatten([fun change_shutdown_timeout_instr/1,
                   fun update_version_instr/1,
                   change_gospel_instr_fun(Config),
                   case orddict:size(Nodes) > orddict:size(ConfigNodes) of
                       true  -> [fun add_node_to_config_instr/1];
                       false -> []
                   end,
                   case orddict:size(ConfigNodes) > 0 of
                       true  -> [fun remove_node_from_config_instr/1];
                       false -> []
                   end]).

create_node_instructions() ->
    [fun create_node_instr/1].

modify_node_instructions(#node { state = off }, _Test) ->
    [fun reset_node_instr/2,
     fun start_node_with_config_instr/2,
     fun start_node_instr/2];
modify_node_instructions(#node { name = Name, state = reset },
                         #test { config = Config }) ->
    [fun start_node_with_config_instr/2
     | case contains_node(Name, Config) of
           true  -> [fun start_node_instr/2];
           false -> []
       end];
modify_node_instructions(#node { }, _Test) ->
    %% NodeState =:= ready orelse NodeState =:= {pending_shutdown, _} ->
    [fun apply_config_instr/2,
     fun stop_node_instr/2].

generate_name(Test = #test { namer = {N, Host} }) ->
    {list_to_atom(lists:flatten(io_lib:format("node~p@~s", [N, Host]))),
     Test #test { namer = {N+1, Host} }}.

noop(Test) -> {noop, Test}.
noop(Node, _Config) -> {noop, Node}.

noops(#test { nodes = Nodes }) ->
    lists:duplicate(orddict:size(Nodes) div 2, fun noop/1).

choose_one_noop(_List, Test = #test { seed = 0 }) ->
    {fun noop/1, Test};
choose_one_noop(List, Test) ->
    choose_one(List ++ noops(Test), Test).

choose_one_noop2(_List, Test = #test { seed = 0 }) ->
    {fun noop/2, Test};
choose_one_noop2(List, Test) ->
    choose_one(List ++ noops(Test), Test).

if_else_noop(noop, Test, _Else) ->
    {noop, Test};
if_else_noop(_, _Test, Else) ->
    Else.

choose_one(_List, Test = #test { seed = 0 }) ->
    {noop, Test};
choose_one(List, Test = #test { seed = Seed }) ->
    Len = length(List),
    {lists:nth(1 + (Seed rem Len), List), Test #test { seed = Seed div Len }}.

contains_node(Node, #config { nodes = Nodes }) -> orddict:is_key(Node, Nodes).

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

add_node_to_config_instr(Test = #test { config = Config =
                                            #config { nodes = ConfigNodes },
                                        nodes  = Nodes }) ->
    Values = [V || V <- orddict:fetch_keys(Nodes),
                   not orddict:is_key(V, ConfigNodes)],
    {Value, Test1} = choose_one(Values, Test),
    Config1 =
        Config #config { nodes = orddict:store(Value, disc, ConfigNodes) },
    if_else_noop(Value, Test,
                 {{config_add_node, Value}, Test1 #test { config = Config1 }}).

remove_node_from_config_instr(
  Test = #test { config = Config = #config { nodes  = Nodes,
                                             gospel = Gospel } }) ->
    Values = [N || N <- orddict:fetch_keys(Nodes), {node, N} =/= Gospel],
    {Value, Test1} = choose_one(Values, Test),
    Config1 = Config #config { nodes = orddict:erase(Value, Nodes) },
    if_else_noop(Value, Test,
                 {{config_remove_node, Value}, Test1 #test { config = Config1 }}).

change_gospel_instr_fun(#config { gospel = Gospel, nodes = Nodes }) ->
    case [{node, N} || N <- orddict:fetch_keys(Nodes)] =:= Gospel
        orelse Nodes =:= [] of
        true  -> [];
        false -> [fun change_gospel_instr/1]
    end.

change_gospel_instr(
  Test = #test { config = Config = #config { nodes  = Nodes,
                                             gospel = Gospel } }) ->
    Values = [reset | [{node, N} || N <- orddict:fetch_keys(Nodes)]],
    {Value, Test1} = choose_one([V || V <- Values, V =/= Gospel], Test),
    Config1 = Config #config { gospel = Value },
    if_else_noop(Value, Test,
                 {{config_gospel_to, Value}, Test1 #test { config = Config1 }}).

change_shutdown_timeout_instr(
  Test = #test { config = Config = #config { shutdown_timeout = ST } }) ->
    Values = [infinity, 0, 1, 2, 10, 30],
    {Value, Test1} = choose_one([V || V <- Values, V =/= ST], Test),
    Config1 = Config #config { shutdown_timeout = Value },
    if_else_noop(Value, Test,
                 {{config_shutdown_timeout_to, Value}, Test1 #test { config = Config1 }}).

update_version_instr(
  Test = #test { config = Config = #config { version = V } }) ->
    Config1 = Config #config { version = V + 1 },
    {{config_version_to, V + 1}, Test #test { config = Config1 }}.

create_node_instr(Test) ->
    {Name, Test1 = #test { nodes = Nodes }} = generate_name(Test),
    Node = #node { name  = Name,
                   pid   = undefined,
                   state = reset },
    {{create_node, Name},
     Test1 #test { nodes = orddict:store(Name, Node, Nodes) }}.

reset_node_instr(Node = #node { name = Name }, _Config) ->
    {{reset_node, Name}, Node #node { state = reset }}.

start_node_instr(Node = #node { name = Name }, Config) ->
    {{start_node, Name}, set_node_state(Node, Config)}.

start_node_with_config_instr(Node = #node { name = Name }, Config) ->
    {{start_node_with_config, Name, Config}, set_node_state(Node, Config)}.

apply_config_instr(Node = #node { name = Name }, Config) ->
    {{apply_config_to_node, Name, Config}, set_node_state(Node, Config)}.

stop_node_instr(Node = #node { name = Name }, _Config) ->
    {{stop_node, Name}, Node #node { state = off }}.

set_node_state(Node = #node { name = Name },
               Config = #config { shutdown_timeout = ST }) ->
    case contains_node(Name, Config) of
         true  -> Node #node { state = ready };
         false -> Node #node { state = {pending_shutdown, ST} }
    end.

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

test() ->
    case node() of
        'nonode@nohost' -> {error, must_be_distributed_node};
        _               -> test(0)
    end.

test(Seed) ->
    [$@|Host] = lists:dropwhile(fun (C) -> C =/= $@ end, atom_to_list(node())),
    test(Host, Seed).

test(Host, Seed) ->
    generate_program(
      #test { seed   = Seed,
              namer  = {0, Host},
              nodes  = orddict:new(),
              config = #config { nodes            = [],
                                 gospel           = reset,
                                 shutdown_timeout = infinity,
                                 version          = 1 }
            }).
