%% The contents of this file are subject to the Mozilla Public License 
%% Version 1.1 (the "License"); you may not use this file except in 
%% compliance with the License. You may obtain a copy of the License at 
%% https://www.mozilla.org/MPL/1.1/ 
%%
%% Software distributed under the License is distributed on an "AS IS" 
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the 
%% License for the specific language governing rights and limitations 
%% under the License. 
%%
%% The Original Code is RabbitMQ. 
%%
%% The Initial Developer of the Original Code is Pivotal Software, Inc. 
%% Portions created by the Initial Developer are Copyright (C) 2013-2016
%% Pivotal Software, Inc. All Rights Reserved.

-module(rabbit_clusterer).

-behaviour(application).

-export([boot/0]).

-export([apply_config/0, apply_config/1,   %% for 'rabbitmqctl eval ...'
         status/0, status/1]).

-export([start/2, stop/1]).

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
                {"Clusterer is pre-booting. ~p~n", undefined, []};
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
