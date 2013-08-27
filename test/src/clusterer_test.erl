-module(clusterer_test).

-export([test/0, test/1]).

-include("clusterer_test.hrl").

test() ->
    case node() of
        'nonode@nohost' -> {error, must_be_distributed_node};
        _               -> test(0)
    end.

test(Seed) ->
    [$@|Host] = lists:dropwhile(fun (C) -> C =/= $@ end, atom_to_list(node())),
    test(Host, Seed).

test(Host, Seed) ->
    State = #test { seed          = Seed,
                    namer         = {0, Host},
                    nodes         = orddict:new(),
                    config        = #config { nodes            = [],
                                              gospel           = reset,
                                              shutdown_timeout = infinity,
                                              version          = 0 },
                    valid_config  = undefined,
                    active_config = undefined
                  },
    Program = clusterer_program:generate_program(State),
    case starts_nodes(Program) of
        true ->
            io:format("Starting interesting program number ~p:~n~p~n~n",
                      [Seed, Program]),
            case clusterer_interpreter:run_program(Program, State) of
                ok -> {Program, success};
                E  -> io:format("Error encountered with program~n~p~n~p~n~n",
                                [Program, E]),
                      {Program, E}
            end;
        false ->
            uninteresting
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
