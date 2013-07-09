-module(rabbit_clusterer_comms_sup).

-behaviour(supervisor).

-export([start_link/0, start_comms/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_comms() ->
    supervisor:start_child(?SERVER, []).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{comms, {rabbit_clusterer_comms, start_link, []},
            temporary, 16#ffffffff, worker, [rabbit_clusterer_comms]}]}}.
