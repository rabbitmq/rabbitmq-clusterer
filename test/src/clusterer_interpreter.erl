-module(clusterer_interpreter).

-export([run_program/2]).

-include("clusterer_test.hrl").

run_program([], InitialState) ->
    {ok, InitialState};
run_program([Step | Steps], InitialState) ->
    PredictedState = Step #step.final_state,
    AchievedStep = run_step(Step #step { final_state = InitialState }),
    ObservedState = await_stability(AchievedStep #step.final_state),
    case compare_state(PredictedState,
                       AchievedStep #step.final_state,
                       ObservedState) of
        ok -> run_program(Steps, PredictedState);
        E  -> E
    end.

run_step(Step) ->
    run_modify_config(run_existential_node(run_modify_nodes(Step))).

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

run_modify_nodes(Step = #step { modify_node_instrs = Instrs,
                                final_state        = Test }) ->
    Test1 = lists:foldr(fun run_modify_node_instr/2, Test, Instrs),
    Step #step { final_state = Test1 }.

run_modify_node_instr(noop, Test) ->
    Test;
run_modify_node_instr({reset_node, Name}, Test = #test { nodes = Nodes }) ->
    Node = #node { state = off, pid = Pid } = orddict:fetch(Name, Nodes),
    ok = clusterer_node:reset(Pid),
    clusterer_utils:store_node(Node #node { state = reset }, Test);
run_modify_node_instr({start_node, Name},
                      Test = #test { nodes         = Nodes,
                                     active_config = AConfig }) ->
    Node = #node { state = State, pid = Pid } = orddict:fetch(Name, Nodes),
    true = State =:= off orelse State =:= reset, %% ASSERTION
    ok = clusterer_node:start(Pid),
    clusterer_utils:store_node(
      clusterer_utils:set_node_state(Node, AConfig), Test);
run_modify_node_instr({start_node_with_config_instr, Name, VConfig},
                      Test = #test { nodes        = Nodes,
                                     valid_config = VConfig }) ->
    Node = #node { state = State, pid = Pid } = orddict:fetch(Name, Nodes),
    true = State =:= off orelse State =:= reset, %% ASSERTION
    ok = clusterer_node:start_with_config(Pid, VConfig),
    clusterer_utils:make_config_active(
      clusterer_utils:store_node(Node #node { state = ready }, Test));
run_modify_node_instr({apply_config_to_node, Name, VConfig},
                      Test = #test { nodes        = Nodes,
                                     valid_config = VConfig }) ->
    %% Now it's possible that the program thought the node would still
    %% be in {pending_shutdown, _} but too much time has passed and
    %% the node has actually stopped. This is fairly easy to fix.
    Node = #node { state = State, pid = Pid } = orddict:fetch(Name, Nodes),
    case State of
        off ->
            ok = clusterer_node:start_with_config(Pid, VConfig);
        {pending_shutdown, _} ->
            ok = clusterer_node:apply_config(Pid, VConfig)
    end,
    clusterer_utils:make_config_active(
      clusterer_utils:store_node(Node #node { state = ready }, Test));
run_modify_node_instr({stop_node, Name}, Test = #test { nodes = Nodes }) ->
    %% Again, we could have thought we should be in {pending_shutdown,
    %% _} but find we're actually stopped. This is fine.
    Node = #node { state = State, pid = Pid } = orddict:fetch(Name, Nodes),
    case State of
        off ->
            Test;
        {pending_shutdown, _} ->
            ok = clusterer_node:stop(Pid),
            clusterer_utils:store_node(Node #node { state = off }, Test)
    end.

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

run_existential_node(Step = #step { existential_node_instr = Instr,
                                    final_state            = Test }) ->
    Test1 = run_existential_node_instr(Instr, Test),
    Step #step { final_state = Test1 }.

run_existential_node_instr(noop, Test) ->
    Test;
run_existential_node_instr({create_node, Name},
                           Test = #test { nodes = Nodes }) ->
    false = orddict:is_key(Name, Nodes), %% ASSERTION
    {ok, Pid} = clusterer_node:start_link(Name),
    Nodes1 = orddict:store(Name, #node { name  = Name,
                                         state = reset,
                                         pid   = Pid }, Nodes),
    Test #test { nodes = Nodes1 };
run_existential_node_instr({delete_node, Name},
                           Test = #test { nodes = Nodes }) ->
    #node { state = State, pid = Pid } = orddict:fetch(Name, Nodes),
    true = State =:= reset orelse State =:= off, %% ASSERTION
    ok = clusterer_node:delete(Pid),
    Test #test { nodes = orddict:erase(Name, Nodes) }.

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

run_modify_config(Step = #step { modify_config_instr = Instr,
                                 final_state         = Test }) ->
    Test1 = run_modify_config_instr(Instr, Test),
    Step #step { final_state = Test1 }.

run_modify_config_instr(noop, Test) ->
    Test;
run_modify_config_instr({config_shutdown_timeout_to, V},
                        Test = #test { config = Config =
                                           #config { shutdown_timeout = V1 } })
  when V =/= V1 ->
    clusterer_utils:set_config(Config #config { shutdown_timeout = V }, Test);
run_modify_config_instr({config_version_to, V},
                        Test = #test { config = Config =
                                           #config { version = V1 } })
  when V > V1 ->
    clusterer_utils:set_config(Config #config { version = V }, Test);
run_modify_config_instr({config_gospel_to, V},
                        Test = #test { config = Config =
                                           #config { gospel = V1 } })
  when V =/= V1 ->
    clusterer_utils:set_config(Config #config { gospel = V }, Test);
run_modify_config_instr({config_add_node, Name},
                        Test = #test { nodes = Nodes,
                                       config = Config =
                                           #config { nodes = ConfigNodes } }) ->
    true  = orddict:is_key(Name, Nodes),       %% ASSERTION
    false = orddict:is_key(Name, ConfigNodes), %% ASSERTION
    ConfigNodes1 = orddict:store(Name, disc, ConfigNodes),
    clusterer_utils:set_config(Config #config { nodes = ConfigNodes1 }, Test);
run_modify_config_instr({config_remove_node, Name},
                        Test = #test { config = Config =
                                           #config { nodes  = ConfigNodes,
                                                     gospel = Gospel } }) ->
    %% We allow nodes to be exterminated even when they're in the
    %% Config. We only require them to be off/reset. So no assertion
    %% for Name in keys(Nodes).
    true = Gospel =/= {node, Name},           %% ASSERTION
    true = orddict:is_key(Name, ConfigNodes), %% ASSERTION
    ConfigNodes1 = orddict:erase(Name, ConfigNodes),
    clusterer_utils:set_config(Config #config { nodes = ConfigNodes1 }, Test).

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<
