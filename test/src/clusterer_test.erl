-module(clusterer_test).

-export([test/1]).

-include("clusterer_test.hrl").

%% NB Limit is exclusive, not inclusive.
test(Limit) when Limit > 0 ->
    case node() of
        'nonode@nohost' -> {error, must_be_distributed_node};
        _               -> test_sequence(0, Limit)
    end.

test_sequence(Limit, Limit) ->
    io:format("No programs between 0 and ~p failed.~n", [Limit]),
    ok;
test_sequence(N, Limit) ->
    case test_program(N) of
        ok  -> test_sequence(N+1, Limit);
        Err -> io:format("Error encountered with program ~p:~n~p~n",
                         [N, Err]),
               Err
    end.

test_program(Seed) ->
    State = new_state(Seed),
    Program = clusterer_program:generate_program(State),
    case filter_program(Program) of
        skip -> ok;
        run  -> io:format("~p...", [Seed]),
                clusterer_interpreter:run_program(Program, State)
    end.

new_state(Seed) ->
    [$@|Host] = lists:dropwhile(fun (C) -> C =/= $@ end, atom_to_list(node())),
    #test { seed          = Seed,
            namer         = {0, Host},
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
    case starts_nodes(Program) of
        true  -> run;
        false -> skip
    end.

starts_nodes(Program) ->
    starts_nodes(Program, orddict:new()).

starts_nodes([], Names) ->
    length(lists:usort(lists:flatten(Names))) > 1;
starts_nodes([#step { modify_node_instrs = Instrs } | Steps], Names) ->
    case [Name || {start_node_with_config, Name, _} <- Instrs] of
        []     -> starts_nodes(Steps, Names);
        Names1 -> starts_nodes(Steps, [Names1 | Names])
    end.
