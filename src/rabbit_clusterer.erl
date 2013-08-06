-module(rabbit_clusterer).

-behaviour(application).

-export([boot/0]).

-export([apply_config/0, apply_config/1]). %% for 'rabbitmqctl eval ...'

-export([start/2, stop/1]).

-rabbit_boot_step({rabbit_clusterer,
                   [{description, "Declarative Clustering"},
                    {mfa, {rabbit_clusterer_coordinator, rabbit_booted, []}},
                    {requires, networking}]}).

%%----------------------------------------------------------------------------

boot() ->
    ok = application:start(rabbitmq_clusterer),
    ok = rabbit_clusterer_coordinator:begin_coordination(),
    ok.

%% Apply_config allows cluster configs to be dynamically applied to a
%% running system. Currently that's best done by rabbitmqctl eval, but
%% may be improved in the future.
apply_config() -> apply_config(undefined).

apply_config(Config) -> rabbit_clusterer_coordinator:apply_config(Config).

%%----------------------------------------------------------------------------

start(normal, []) -> rabbit_clusterer_sup:start_link().

stop(_State) -> ok.

