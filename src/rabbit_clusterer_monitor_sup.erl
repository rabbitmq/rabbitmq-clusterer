-module(rabbit_clusterer_monitor_sup).

-behaviour(supervisor).

-export([start_link/0, start_monitor/1]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_monitor(Config) ->
    {ok, _Pid} = supervisor:start_child(?SERVER, [Config]).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 10, 10},
          [{monitor, {rabbit_clusterer_monitor, start_link, []},
            temporary, 16#ffffffff, worker, [rabbit_clusterer_monitor]}]}}.
