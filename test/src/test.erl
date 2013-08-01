-module(test).

-export([test/0]).

-include("rabbit_clusterer.hrl").

-record(test, { seed,
                program,
                cluster_state
              }).
-record(cluster, { nodes_state,
                   config,
                   configs
                 }).

-record(node_state, { config,
                      running }).

interpret({seq, []}, State) ->
    State;
interpret({seq, [H|T]}, State) ->
    interpret({seq, T}, interpret(H, State));
interpret({par, []}, State) ->
    State;
interpret({par, P}, State) ->
    Ref = make_ref(),
    Self = self(),
    Fun = fun (Result) -> Self ! {Ref, self(), Result}, ok end,
    PidTasks =
        orddict:from_list(
          [{spawn(fun () -> Fun(interpret(T, State)) end), T} || T <- P]),
    merge_results(gather(PidTasks, Ref), State);
interpret({merge_results, Task, TaskState}, State) ->
    %% todo
    State;
interpret({exec, Fun}, State) ->
    Fun(State).

gather(PidTasks, Ref) ->
    MRefs = [monitor(process, Pid) || Pid <- orddict:fetch_keys(PidTasks)],
    Results = gather(PidTasks, Ref, orddict:new()),
    [demonitor(MRef, [flush]) || MRef <- MRefs],
    Results.

gather(PidTasks, Ref, Results) ->
    case orddict:size(PidTasks) of
        0 -> Results;
        _ -> receive
                 {Ref, Pid, Result} ->
                     gather(orddict:erase(Pid, PidTasks), Ref,
                            orddict:store(orddict:fetch(Pid, PidTasks),
                                          Result, Results));
                 {'DOWN', _MRef, process, Pid, _Info} ->
                     gather(orddict:erase(Pid, PidTasks), Ref,
                            orddict:update(orddict:fetch(Pid, PidTasks),
                                           fun id/1, failed, Results))
             end
    end.

merge_results(TaskResults, State) ->
    interpret(
      {seq, [{merge, T, R} || {T, R} <- orddict:to_list(TaskResults)]}, State).

generate(Seed) ->
    Config = #config { nodes            = [],
                       shutdown_timeout = infinity,
                       gospel           = reset,
                       version          = 0 },
    Test = #test { seed          = Seed,
                   program       = [],
                   cluster_state =
                       #cluster { nodes_state = [],
                                  config = Config,
                                  configs = orddict:new()
                                }
                 },
    Test1 = generate_program(Test),
    Test1 #test { seed = Seed }.

generate_program(Test = #test { program = Prog, seed = 0 }) ->
    Test #test { program = lists:reverse(Prog) };
generate_program(Test = #test { program       = Prog,
                                cluster_state = CS,
                                seed          = Seed }) ->
    {{Instr, CS1}, Seed1} = choose_one(
                              Seed,
                              lists:append(
                                [change_shutdown_timeout_instructions(CS),
                                 change_gospel_instructions(CS),
                                 add_node_to_cluster_instructions(CS),
                                 remove_node_from_cluster_instructions(CS),
                                 start_node_instructions(CS),
                                 stop_node_instructions(CS),
                                 delete_node_instructions(CS),
                                 await_termination_instructions(CS),
                                 reset_node_instructions(CS)])),
    generate_program(Test #test { program       = [Instr | Prog],
                                  seed          = Seed1,
                                  cluster_state = CS1 }).

choose_one(N, List) ->
    Len = length(List),
    {lists:nth(1 + (N rem Len), List), N div Len}.

change_shutdown_timeout_instructions(CS = #cluster { config = Config = #config { shutdown_timeout = X }}) ->
    InterestingValues = [infinity, 0, 1, 2, 10, 30],
    [{{change_shutdown_timeout, V},
      CS #cluster { config = Config #config { shutdown_timeout = V }}}
     || V <- InterestingValues, V =/= X ].

change_gospel_instructions(CS = #cluster { config = Config = #config { gospel = X, nodes = Nodes }}) ->
    Values = [reset | [N || {N, _} <- Nodes]],
    [{{change_gospel, V},
      CS #cluster { config = Config #config { gospel = V }}}
     || V <- Values, V =/= X ].

add_node_to_cluster_instructions(CS = #cluster { nodes_state = NodesState,
                                                 config = Config = #config { nodes = ConfigNodes }}) ->
    NewNode = generate_node(orddict:fetch_keys(NodesState)),
    Nodes = [NewNode | [N || {N, _NS} <- NodesState, not orddict:is_key(N, ConfigNodes)]],
    [{{add_node_to_cluster, N},
     CS #cluster { config = Config #config { nodes = [{N, disc} | ConfigNodes] } }} || N <- Nodes].

remove_node_from_cluster_instructions(CS = #cluster { config = Config = #config { nodes = ConfigNodes }}) ->
    [{{remove_node_from_cluster, N},
      CS #cluster { config = Config #config { nodes = orddict:erase(N, ConfigNodes) } }}
     || {N, _} <- ConfigNodes].

start_node_instructions(CS = #cluster { nodes_state = NodesState, config = Config }) ->
    NewNode = generate_node(orddict:fetch_keys(NodesState)),
    %% NewNode has just been freshly generated, so we can guarantee
    %% that it's not mentioned by any of the configs out there.
    Nodes = [{NewNode, #node_state { running = false,
                                     config  = default }}
             | [{N, NS} || {N, NS = #node_state { running = false }} <- NodesState]],
    %% So what config is actually going to apply to our started node?
    %% The simple case is it's started up with Config, which is by
    %% definition the youngest config out there. The other case leads
    %% to a fight to find the youngest between every running config
    %% out there that names the node. Ultimately, the youngest of all
    %% of those will win and propogate. Eg it is possible to have a
    %% config of A+B+C and C+D+E both 'running' with different
    %% versions provided C is down. In truth at least one of those two
    %% would be blocked in starting, I think.
    Update = fun (N, NS = #node_state { config_pending = ConfigPend }, ApplyConfig) ->
                     NewConfig = #config { nodes = ClusterNodes, shutdown_timeout = T } =
                         case ApplyConfig of
                             true  -> Config;
                             false -> case [N || {N, #node_state { config = ConfigPend1 }} <- NodesState,
                                                 ConfigPend1 =:= ConfigPend,
                                                 %% The following is necessary to 
                                                 orddict:is_key(N, ConfigPend #config.nodes)] of
                                          
                         end,
                     NS #node_state { config = NewConfig,
                                      config_pending = NewConfig,
                                      running = case orddict:is_key(N, ClusterNodes) of
                                                    true  -> true;
                                                    false -> {terminating, T}
                                                end }
             end,
    [{{start_node, N, ApplyConfig},
      begin
          CS1 = CS #cluster { nodes_state = orddict:store(N, Update(N, NS, ApplyConfig), NodesState) },
          case ApplyConfig of
              true  -> track_cluster_propogatation(CS1, N);
              false -> CS1
          end
      end} || {N, NS} <- Nodes, ApplyConfig <- [true, false]].

stop_node_instructions(CS = #cluster { nodes_state = NodesState }) ->
    Stops = [{{stop_node, N}, N, NS #node_state { running = false }}
             || {N, NS = #node_state { running = R }} <- NodesState, R =/= false ],
    %% We want to offer stopping the first, the first two, or stopping
    %% all.
    case Stops of
        [] ->
            [];
        [{Instr, N, NS}] ->
            [{Instr, CS #cluster { nodes_state = orddict:store(N, NS, NodesState) }}];
        [{AI,A,ANS},{BI,B,BNS}|Ts] ->
            [{AI, CS #cluster { nodes_state = orddict:store(A, ANS, NodesState) }},
             {{par, [AI, BI]}, CS #cluster { nodes_state =
                                                 orddict:store(
                                                   A, ANS, orddict:store(
                                                             B, BNS, NodesState)) }} |
             case Ts of
                 [] ->
                     [];
                 _  ->
                     [{{par, [I || {I, _, _} <- Stops]},
                       CS #cluster { nodes_state =
                                         lists:foldr(fun ({_Instr, N, NS}, NodesStateN) ->
                                                             orddict:store(N, NS, NodesStateN)
                                                     end, NodesState, Stops) }}]
             end]
    end.

delete_node_instructions(CS = #cluster { nodes_state = NodesState }) ->
    [{{delete_node, N},
      CS #cluster { nodes_state = orddict:erase(N, NodesState) }
     } || {N, #node_state { running = false }} <- NodesState ].

await_termination_instructions(CS = #cluster { nodes_state = NodesState }) ->
    [{{await_termination, N, T},
      CS #cluster { nodes_state =
                        orddict:store(N, NS #node_state { running = false }, NodesState) }
     } || {N, NS = #node_state { running = {terminating, T} }} <- NodesState, T =/= infinity ].

reset_node_instructions(CS = #cluster { nodes_state = NodesState,
                                        config = Config = #config { nodes = ConfigNodes } }) ->
    [{{reset_node, N},
      CS #cluster { nodes_state = orddict:store(
                                    N, NS #node_state { config = default }, NodesState) }
     } || {N, NS = #node_state { config = C, running = false }} <- NodesState, C =/= default].

track_cluster_propogatation(CS = #cluster { nodes_state = NodesState,
                                            config = Config }, Node) ->
    CS #cluster { nodes_state = track_cluster_propogatation(Config, NodesState, [Node]) }.

track_cluster_propogatation(_Config, NodesState, []) ->
    NodesState;
track_cluster_propogatation(Config, NodesState, [Node|WL]) ->
    NewRunning = case orddict:is_key(Node, Config #config.nodes) of
                     true  -> true;
                     false -> {terminating, Config #config.shutdown_timeout}
                 end,
    case orddict:fetch(Node, NodesState) of
        #node_state { config = Config } ->
            %% Already been here
            track_cluster_propogatation(Config, NodesState, WL);
        #node_state { running = false } ->
            track_cluster_propogatation(Config, NodesState, WL);
        #node_state { running = {terminating, _T} } = NS ->
            %% It will get updated to the new config, but it won't
            %% pass that on to the members of its current config.
            track_cluster_propogatation(
              Config, orddict:store(Node,
                                    NS #node_state { config = Config, running = NewRunning },
                                    NodesState), WL);
        #node_state { running = true, config = #config { nodes = Nodes } } = NS ->
            WL1 = [N || {N,_} <- Nodes, N =/= Node] ++ WL,
            %% New config will propogate to all members of the old config.
            track_cluster_propogatation(
              Config, orddict:store(Node,
                                    NS #node_state { config = Config, running = NewRunning },
                                    NodesState), WL1)
    end.

id(X) -> X.

test() ->
    case node() of
        'nonode@nohost' ->
            {error, must_be_distributed_node};
        _ ->
            ok
    end.
