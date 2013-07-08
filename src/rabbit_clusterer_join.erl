-module(rabbit_clusterer_join).

-export([init/1, event/2]).

-record(state, { config }).

init(Config) ->
    {continue, #state { config = Config }}.
    

event(Event, Config) ->
    {continue, Config}.
