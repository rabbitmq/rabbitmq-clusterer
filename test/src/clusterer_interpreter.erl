-module(clusterer_interpreter).

-export([run_program/1]).

-include("clusterer_test.hrl").

-define(SLEEP, timer:sleep(500)).

%%----------------------------------------------------------------------------

run_program({InitialState, Steps}) ->
    run_program(Steps, InitialState).

run_program([], FinalState) ->
    ok = tidy(FinalState),
    ok;
run_program([Step | Steps], InitialState) ->
    PredictedState = Step #step.final_state,
    AchievedState = (run_step(Step #step { final_state = InitialState })
                    ) #step.final_state,
    case check_convergence(PredictedState, AchievedState) of
        ok ->
            case compare_state(AchievedState,
                               observe_stable_state(AchievedState)) of
                {ok, ObservedState} -> run_program(Steps, ObservedState);
                E1                  -> E1
            end;
        {error, E2} ->
            ok = tidy(AchievedState),
            {error, E2, Step}
    end.

run_step(Step) ->
    run_modify_config(run_existential_node(run_modify_nodes(Step))).

tidy(#state { nodes = Nodes }) ->
    [clusterer_node:exit(Pid)
     || {_Name, #node { pid = Pid }} <- orddict:to_list(Nodes)],
    ok.

%%----------------------------------------------------------------------------

check_convergence(#state { nodes         = NodesPred,
                           config        = Config,
                           valid_config  = VConfig,
                           active_config = AConfig },
                  #state { nodes         = NodesAchi,
                           config        = Config,
                           valid_config  = VConfig,
                           active_config = AConfig }) ->
    %% Configs should just match exactly. Nodes will differ only in
    %% that Achi will have pids
    case {orddict:fetch_keys(NodesPred), orddict:fetch_keys(NodesAchi)} of
        {Eq, Eq} ->
            orddict:fold(
              fun (_Name, _Node, {error, _} = Err) ->
                      Err;
                  (Name, #node { name = Name, state = StateAchi }, ok) ->
                      #node { name = Name, state = StatePred } =
                          orddict:fetch(Name, NodesPred),
                      case {StatePred, StateAchi} of
                          {EqSt,                  EqSt} -> ok;
                          {_,                     _   } ->
                              {error, {node_state_divergence, Name,
                                       StateAchi, StatePred}}
                      end
              end, ok, NodesAchi);
        {Pr, Ac} ->
            {error, {node_divergence, Pr, Ac}}
    end;
check_convergence(Pred, Achi) ->
    {error, {config_divergence, Pred, Achi}}.

observe_stable_state(State = #state { nodes = Nodes }) ->
    Pids = [Pid || {_Name, #node { pid = Pid }} <- orddict:to_list(Nodes)],
    case clusterer_node:observe_stable_state(Pids) of
        {stable, S} -> ?SLEEP, %% always sleep, just to allow some time
                       case clusterer_node:observe_stable_state(Pids) of
                           {stable, S} -> S; %% No one has changed, all good.
                           _           -> observe_stable_state(State)
                       end;
        _           -> ?SLEEP,
                       observe_stable_state(State)
    end.

compare_state(State = #state { nodes         = Nodes,
                               active_config = AConfig }, StableState) ->
    case {orddict:fetch_keys(Nodes), orddict:fetch_keys(Nodes)} of
        {Eq, Eq} ->
            Result =
                orddict:fold(
                  fun (_Name, _Node, {error, _} = Err) ->
                          Err;
                      (Name, Node = #node { name = Name, state = NS }, Acc) ->
                          Observed = orddict:fetch(Name, StableState),
                          case {NS, Observed} of
                              {off, off} ->
                                  orddict:store(Name, Node, Acc);
                              {reset, reset} ->
                                  orddict:store(Name, Node, Acc);
                              {ready, {ready, AConfig}} ->
                                  orddict:store(Name, Node, Acc);
                              {_, _} = DivergenceSt ->
                                  {error, {node_state_divergence, DivergenceSt}}
                          end
                  end, orddict:new(), Nodes),
            case Result of
                {error, _} = Err -> Err;
                Nodes1           -> {ok, State #state { nodes = Nodes1 }}
            end;
        {_, _} = DivergenceNodes ->
            {error, {nodes_divergence, DivergenceNodes}}
    end.

%%----------------------------------------------------------------------------

run_modify_nodes(Step = #step { modify_node_instrs = Instrs,
                                final_state        = State }) ->
    State1 = lists:foldr(fun run_modify_node_instr/2, State, Instrs),
    Step #step { final_state = State1 }.

run_modify_node_instr(noop, State) ->
    State;
run_modify_node_instr({reset_node, Name}, State = #state { nodes = Nodes }) ->
    Node = #node { state = off, pid = Pid } = orddict:fetch(Name, Nodes),
    ok = clusterer_node:reset(Pid),
    clusterer_utils:store_node(Node #node { state = reset }, State);
run_modify_node_instr({start_node, Name},
                      State = #state { nodes         = Nodes,
                                       active_config = AConfig }) ->
    Node = #node { state = NS, pid = Pid } = orddict:fetch(Name, Nodes),
    true = NS =:= off orelse NS =:= reset, %% ASSERTION
    ok = clusterer_node:start(Pid),
    clusterer_utils:store_node(clusterer_utils:set_node_state(
                                 Node #node { state = ready }, AConfig), State);
run_modify_node_instr({start_node_with_config, Name, VConfig},
                      State = #state { nodes        = Nodes,
                                       valid_config = VConfig }) ->
    Node = #node { state = NS, pid = Pid } = orddict:fetch(Name, Nodes),
    true = NS =:= off orelse NS =:= reset, %% ASSERTION
    ok = clusterer_node:start_with_config(Pid, VConfig),
    clusterer_utils:make_config_active(
      clusterer_utils:store_node(Node #node { state = ready }, State));
run_modify_node_instr({apply_config_to_node, Name, VConfig},
                      State = #state { nodes        = Nodes,
                                       valid_config = VConfig }) ->
    Node = #node { state = ready, pid = Pid } = orddict:fetch(Name, Nodes),
    ok = clusterer_node:apply_config(Pid, VConfig),
    clusterer_utils:make_config_active(clusterer_utils:store_node(Node, State));
run_modify_node_instr({stop_node, Name}, State = #state { nodes = Nodes }) ->
    Node = #node { state = ready, pid = Pid } = orddict:fetch(Name, Nodes),
    ok = clusterer_node:stop(Pid),
    clusterer_utils:store_node(Node #node { state = off }, State).

%%----------------------------------------------------------------------------

run_existential_node(Step = #step { existential_node_instr = Instr,
                                    final_state            = State }) ->
    State1 = run_existential_node_instr(Instr, State),
    Step #step { final_state = State1 }.

run_existential_node_instr(noop, State) ->
    State;
run_existential_node_instr({create_node, Name, Port},
                           State = #state { nodes = Nodes }) ->
    false = orddict:is_key(Name, Nodes), %% ASSERTION
    {ok, Pid} = clusterer_node:start_link(Name, Port),
    Nodes1 = orddict:store(Name, #node { name  = Name,
                                         port  = Port,
                                         state = reset,
                                         pid   = Pid }, Nodes),
    State #state { nodes = Nodes1 };
run_existential_node_instr({delete_node, Name},
                           State = #state { nodes = Nodes }) ->
    #node { state = NS, pid = Pid } = orddict:fetch(Name, Nodes),
    true = NS =:= reset orelse NS =:= off, %% ASSERTION
    ok = clusterer_node:delete(Pid),
    State #state { nodes = orddict:erase(Name, Nodes) }.

%%----------------------------------------------------------------------------

run_modify_config(Step = #step { modify_config_instr = Instr,
                                 final_state         = State }) ->
    State1 = run_modify_config_instr(Instr, State),
    Step #step { final_state = State1 }.

run_modify_config_instr(noop, State) ->
    State;
run_modify_config_instr({config_version_to, V},
                        State = #state { config = Config =
                                             #config { version = V1 } })
  when V > V1 ->
    clusterer_utils:set_config(Config #config { version = V }, State);
run_modify_config_instr({config_gospel_to, V},
                        State = #state { config = Config =
                                             #config { gospel = V1 } })
  when V =/= V1 ->
    clusterer_utils:set_config(Config #config { gospel = V }, State);
run_modify_config_instr({config_add_node, Name},
                        State = #state { nodes = Nodes,
                                         config = Config =
                                             #config { nodes = ConfigNodes } }) ->
    true  = orddict:is_key(Name, Nodes),       %% ASSERTION
    false = orddict:is_key(Name, ConfigNodes), %% ASSERTION
    ConfigNodes1 = orddict:store(Name, disc, ConfigNodes),
    clusterer_utils:set_config(Config #config { nodes = ConfigNodes1 }, State);
run_modify_config_instr({config_remove_node, Name},
                        State = #state { config = Config =
                                             #config { nodes  = ConfigNodes,
                                                       gospel = Gospel } }) ->
    %% We allow nodes to be exterminated even when they're in the
    %% Config. We only require them to be off/reset. So no assertion
    %% for Name in keys(Nodes).
    true = Gospel =/= {node, Name},           %% ASSERTION
    true = orddict:is_key(Name, ConfigNodes), %% ASSERTION
    ConfigNodes1 = orddict:erase(Name, ConfigNodes),
    clusterer_utils:set_config(Config #config { nodes = ConfigNodes1 }, State).
