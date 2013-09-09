-module(clusterer_test).

-export([test/1, test/2, test_program/1]).

-include("clusterer_test.hrl").

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
test_program(NomadicProgram) when is_list(NomadicProgram) ->
    {_, Host} = rabbit_nodes:parts(node()),
    Prog = clusterer_utils:localise_program(NomadicProgram, Host),
    State = new_state(unknown_seed),
    {NomadicProgram, clusterer_interpreter:run_program(Prog, State)}.

test_program(Host, Seed) ->
    State = new_state(Seed),
    NomadicProgram = clusterer_program:generate_program(State),
    case filter_program(NomadicProgram) of
        skip -> skip;
        run  -> Prog = clusterer_utils:localise_program(NomadicProgram, Host),
                {NomadicProgram, clusterer_interpreter:run_program(Prog, State)}
    end.

%%----------------------------------------------------------------------------

new_state(Seed) ->
    #test { seed          = Seed,
            node_count    = 0,
            nodes         = orddict:new(),
            config        = #config { nodes            = [],
                                      gospel           = reset,
                                      shutdown_timeout = infinity,
                                      version          = 0 },
            valid_config  = undefined,
            active_config = undefined
          }.

filter_program(Program) ->
    %% Eventually there'll be a more sophisticated set of filters here.
    case two_ready(Program) of
        true  -> run;
        false -> skip
    end.

two_ready(Steps) ->
    lists:any(fun (#step { final_state = #test { nodes = Nodes } }) ->
                      length([true || {_Name, #node { state = ready }}
                                          <- orddict:to_list(Nodes)]) > 1
              end, Steps).
