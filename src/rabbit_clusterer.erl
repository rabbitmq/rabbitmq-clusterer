-module(rabbit_clusterer).

-behaviour(application).

-export([start/2, stop/1]).

-export([await_clustering/0, ready_to_cluster/0]).

-rabbit_boot_step(
   {rabbit_clusterer_p1,
    [{description, "Declarative Clustering - part 1"},
     {mfa, {?MODULE, await_clustering, []}},
     {requires, file_handle_cache},
     {enables, database}]}).

-rabbit_boot_step(
   {rabbit_clusterer_p2,
    [{description, "Declarative Clustering - part 2"},
     {mfa, {?MODULE, ready_to_cluster, []}},
     {requires, external_infrastructure}]}).

-define(APP, rabbitmq_clusterer).

start(normal, []) ->
    rabbit_clusterer_sup:start_link().

stop(_State) ->
    ok.

await_clustering() ->
    %% We need to ensure the app is already started:
    ok = application:ensure_started(?APP),
    %% deliberate badmatch against shutdown. TODO tidy/improve
    ok = rabbit_clusterer_coordinator:await_coordination(),
    ok.

ready_to_cluster() ->
    ok = rabbit_clusterer_coordinator:ready_to_cluster().
