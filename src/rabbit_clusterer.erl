-module(rabbit_clusterer).

-behaviour(application).

-export([start/2, stop/1, boot/0, apply_config/0, apply_config/1]).

-export([begin_clustering/0, rabbit_booted/0]).

-rabbit_boot_step(
   {rabbit_clusterer,
    [{description, "Declarative Clustering"},
     {mfa, {?MODULE, rabbit_booted, []}},
     {requires, networking}]}).

-define(APP, rabbitmq_clusterer).

start(normal, []) ->
    rabbit_clusterer_sup:start_link().

stop(_State) ->
    ok.

begin_clustering() ->
    ok = rabbit_clusterer_utils:stop_mnesia(),
    %% We need to ensure the app is already started:
    ok = application:ensure_started(?APP),
    %% deliberate badmatch against shutdown. TODO tidy/improve
    ok = rabbit_clusterer_coordinator:begin_coordination(),
    ok.

rabbit_booted() ->
    ok = rabbit_clusterer_coordinator:rabbit_booted().

boot() ->
    ok = begin_clustering().

apply_config() ->
    apply_config(undefined).

apply_config(Config) ->
    rabbit_clusterer_coordinator:apply_config(Config).
