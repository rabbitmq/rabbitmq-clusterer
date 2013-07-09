-module(rabbit_clusterer_sup).

-behaviour(supervisor2).

-export([start_link/0, init/1]).

start_link() ->
    supervisor2:start_link(?MODULE, []).

init([]) ->
    {ok, {{one_for_all, 0, 1},
          [{rabbit_clusterer_coordinator,
            {rabbit_clusterer_coordinator, start_link, []},
            intrinsic, 16#ffffffff, worker, [rabbit_clusterer_coordinator]},
           {rabbit_clusterer_comms_sup,
            {rabbit_clusterer_comms_sup, start_link, []},
            intrinsic, infinity, supervisor, [rabbit_clusterer_comms_sup]}]}}.
