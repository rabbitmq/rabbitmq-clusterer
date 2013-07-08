-module(rabbit_clusterer).

-behaviour(application).

-export([start/2, stop/1]).

-export([await_clustering/0]).

-rabbit_boot_step(
   {rabbit_clusterer,
    [{description, "Declarative Clustering"},
     {mfa, {?MODULE, await_clustering, []}},
     {enables, database}]}).

-define(APP, rabbitmq_clusterer).

start(normal, []) ->
    rabbit_clusterer_sup:start_link().

stop(_State) ->
    ok.

await_clustering() ->
    %% We need to ensure the app is already started:
    case application:start(?APP) of
        ok                               -> ok;
        {error, {already_started, ?APP}} -> ok
    end,
    rabbit_clusterer_coordinator:await_coordination().
