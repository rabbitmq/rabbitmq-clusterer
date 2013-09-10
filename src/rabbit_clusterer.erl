-module(rabbit_clusterer).

-behaviour(application).

-export([boot/0]).

-export([apply_config/0, apply_config/1,   %% for 'rabbitmqctl eval ...'
         status/0, status/1]).

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

status() ->
    status(node()).

status(Node) ->
    {Message, Config, List} =
        case rabbit_clusterer_coordinator:request_status(Node) of
            preboot ->
                {"Clusterer is booting.~n", undefined, []};
            {Config1, booting} ->
                {"Clusterer is booting Rabbit into cluster configuration: "
                 "~n~s~n", Config1, []};
            {Config1, ready} ->
                {"Rabbit is running in cluster configuration: ~n~s~n"
                 "Running nodes: ~p~n", Config1,
                [rabbit_mnesia:cluster_nodes(running)]};
            {Config1, {transitioner, join}} ->
                {"Clusterer is trying to join into cluster configuration: "
                 "~n~s~n", Config1, []};
            {Config1, {transitioner, rejoin}} ->
                {"Clusterer is trying to rejoin cluster configuration: ~n~s~n",
                 Config1, []}
        end,
    Config2 = case Config of
                  undefined -> "";
                  _         -> rabbit_misc:format(
                                 "~p", [tl(rabbit_clusterer_config:to_proplist(
                                             undefined, Config))])
              end,
    io:format(Message, [Config2 | List]).

%%----------------------------------------------------------------------------

start(normal, []) -> rabbit_clusterer_sup:start_link().

stop(_State) -> ok.
