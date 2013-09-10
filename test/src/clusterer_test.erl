-module(clusterer_test).

-export([test/1, test/2, test_program/1]).

-include("clusterer_test.hrl").

%%----------------------------------------------------------------------------
%%
%% Testing the Clusterer
%%
%% Testing the Clusterer is challenging given the level of
%% concurrency, eventually-consistent design, and the vast number of
%% scenarios and modifications possible. The approach we take here is
%% to deterministically generate programs which are then filtered and
%% interpreted. These programs describe how to construct up to one
%% cluster and operations upon the cluster.
%%
%% A program is a sequence of steps from a given starting state. Each
%% step contains up to one instruction for each node, up to one
%% instruction for modifying the cluster config, and up to one
%% instruction for creating or deleting a node. All instructions are
%% independent thus all the instructions within a step can be run in
%% parallel. For example, a step can not contain both a "switch node X
%% on" and a "delete node X" instruction. During program generation we
%% capture the expected state of all the nodes at the end of each
%% step. When we come to interpret the program, we use this expected
%% state to compare to what we observe of the Real World. If we reach
%% the end of the program and no divergence is observed then the
%% program passed successfully.
%%
%% Program generation is driven by a seed. When the seed is reduced to
%% 0, there is no more entropy available, and so program generation
%% halts. At each step, the set of available instructions to choose
%% from is highly dependent on the predicted state of the nodes at
%% this point. Having constructed a list of viable instructions at a
%% given point in the program, the seed is used to select the
%% instruction. The list will always include the 'noop' instruction
%% (but will never be just the 'noop' instruction), and the seed
%% modulo the length of the list of valid instructions is used to
%% select which instruction is chosen. The new seed is the current
%% seed divided by the length of that list and is passed to the next
%% stage of instruction selection. Thus the entropy of the seed is
%% slowly reduced: it is consumed by selecting instructions from
%% lists. This strategy means every seed will result in a unique
%% program (though isomorphisms of various degrees are possible), and
%% that a given seed will always generate the same program. In other
%% words, the seed is equivalent to the perfect compression of the
%% program (proof left as an exercise to the reader...).
%%
%% Program generation is heavily constrained to avoid generating
%% programs where there are multiple possible valid outcomes, and to
%% ensure we never have more than a single cluster running. The latter
%% is essential to keep modelling of the nodes feasible.
%%
%% Having generated a program we then filter it to test whether or not
%% in contains any interesting aspects. Currently we just look for
%% programs which contain more than a single node running at any given
%% point in time, but more elaborate filters are possible. Having
%% selected a program to run, it is then passed to the interpreter.
%%
%% Some aspects of the interpreter are remarkably similar to the
%% program generation itself: in the program generator we have to
%% model changes to nodes and cluster configs and certainly much of
%% the mechanism for making changes to the cluster configs is very
%% similar in the interpreter. For nodes, we have one process per node
%% to allow the possibility of changes to the nodes themselves for
%% each step actually occurring in parallel. In this regard, given the
%% program doesn't really specify scheduling of instructions beyond
%% "concurrently", multiple different runs of the same program may
%% result in different instructions being evaluated at different
%% times. However, the point of the Clusterer (and the constraints of
%% the program generation) is that it should be eventually consistent:
%% the outcome of each step should be the independent of individual
%% scheduling of instructions within a step.
%%
%% To detect divergence we have to be aware that a cluster of nodes
%% may take a short period of time to stabilise at a new cluster
%% config, to turn off, etc. The strategy we adopt is not fool-proof,
%% but it'll do. We wait for all nodes to be stable in some way
%% (i.e. off, reset, on or pending_shutdown, but not booting). We then
%% wait a short amount of time and ask them all again for their
%% state. If they're all still stable and they're all still stable in
%% the same way, then we declare the cluster is stable and that we can
%% actually check this state for divergence. At this point we compare
%% their stable states and the cluster configs they're running with
%% the state our program generation predicted for each step and fail
%% if any divergence is detected.
%%
%%----------------------------------------------------------------------------

%% NB Limit is exclusive, not inclusive.
test(Limit) when Limit > 0 ->
    test(0, Limit).

test(From, To) when To > From ->
    case node() of
        'nonode@nohost' -> {error, must_be_distributed_node};
        Node            -> {_, Host} = rabbit_nodes:parts(Node),
                           io:format("Passed programs: ["),
                           test_sequence(Host, To, From, 0)
    end.

test_sequence(_Host, Limit, Limit, RanCount) ->
    io:format("].~n~p programs were ran and passed~n", [RanCount]),
    ok;
test_sequence(Host, Limit, N, RanCount) ->
    case test_program(Host, N) of
        skip           -> test_sequence(Host, Limit, N+1, RanCount);
        {_Program, ok} -> io:format("~p,", [N]),
                          test_sequence(Host, Limit, N+1, RanCount+1);
        {Program, Err} -> io:format("~nError encountered with program ~p:"
                                    "~n~n~p~n~n~p~n", [N, Program, Err]),
                          Err
    end.

test_program(Seed) when is_integer(Seed) ->
    {_, Host} = rabbit_nodes:parts(node()),
    test_program(Host, Seed);
test_program(NomadicProgram = {#state {}, Steps}) when is_list(Steps) ->
    {_, Host} = rabbit_nodes:parts(node()),
    Prog = clusterer_utils:localise_program(NomadicProgram, Host),
    {NomadicProgram, clusterer_interpreter:run_program(Prog)}.

test_program(Host, Seed) ->
    NomadicProgram = clusterer_program:generate_program(new_state(Seed)),
    case filter_program(NomadicProgram) of
        skip -> skip;
        run  -> Prog = clusterer_utils:localise_program(NomadicProgram, Host),
                {NomadicProgram, clusterer_interpreter:run_program(Prog)}
    end.

%%----------------------------------------------------------------------------

new_state(Seed) ->
    #state { seed          = Seed,
             node_count    = 0,
             nodes         = orddict:new(),
             config        = #config { nodes   = [],
                                       gospel  = reset,
                                       version = 0 },
             valid_config  = undefined,
             active_config = undefined
           }.

filter_program(Program) ->
    %% Eventually there'll be a more sophisticated set of filters here.
    case two_ready(Program) of
        true  -> run;
        false -> skip
    end.

two_ready({_InitialState, Steps}) ->
    lists:any(fun (#step { final_state = #state { nodes = Nodes } }) ->
                      length([true || {_Name, #node { state = ready }}
                                          <- orddict:to_list(Nodes)]) > 1
              end, Steps).
