-module(rabbit_clusterer).

-behaviour(application).

-export([start/2, stop/1, boot/0]).

-export([apply_config/0, apply_config/1]). %% for 'rabbitmqctl eval ...'

-export([rabbit_booted/0]).

-rabbit_boot_step({rabbit_clusterer,
                   [{description, "Declarative Clustering"},
                    {mfa, {?MODULE, rabbit_booted, []}},
                    {requires, networking}]}).

-define(APP, rabbitmq_clusterer).

start(normal, []) -> rabbit_clusterer_sup:start_link().

stop(_State) -> ok.

boot() ->
    ok = rabbit_clusterer_utils:stop_mnesia(),
    %% We need to ensure the app is already started:
    ok = application:ensure_started(?APP),
    ok = rabbit_clusterer_coordinator:begin_coordination(),
    ok.

rabbit_booted() ->
    ok = rabbit_clusterer_coordinator:rabbit_booted().

%% Apply_config allows cluster configs to be dynamically applied to a
%% running system. Currently that's best done by rabbitmqctl eval, but
%% may be improved in the future.
apply_config() -> apply_config(undefined).

apply_config(Config) -> rabbit_clusterer_coordinator:apply_config(Config).
