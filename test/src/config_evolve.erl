-module(config_evolve).

-behaviour(gen_server).

-export([observe/2, transmogrify/1, fetch/1]).

-export([start_link/3, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {config, node, node_namer}).

-include("rabbit_clusterer.hrl").

-define(NOOP_WEIGHT, 4).

observe(Pid, Seed) ->
    gen_server:call(Pid, {observe, Seed}, infinity).

transmogrify(Pid) ->
    gen_server:call(Pid, transmogrify, infinity).

fetch(Pid) ->
    gen_server:call(Pid, fetch, infinity).

start_link(Node, Config = #config {}, Namer) ->
    {ok, _Pid} = gen_server:start_link(?MODULE, [Node, Config, Namer], []).

init([Node, Config, Namer]) ->
    {ok, #state {config = Config, node = Node, node_namer = Namer}}.

handle_call({observe, Seed}, _From, State = #state { config     = Config,
                                                     node_namer = Namer }) ->
    {Config1, Seed1} = choose_one(
                         Seed,
                         lists:append(
                           [change_shutdown_timeout(Config),
                            change_gospel(Config),
                            add_node_to_cluster(Config, Namer),
                            remove_node_from_cluster(Config),
                            lists:duplicate(?NOOP_WEIGHT, Config)])),
    %% Some of the options are deliberately lazy, so eval here
    Config2 = case is_function(Config1) of
                  true  -> Config1();
                  false -> Config1
              end,
    {reply, Seed1, State #state { config = Config2 }};
handle_call(transmogrify, _From, State) ->
    %% This is really a noop as we apply the selected change in
    %% observe. Application of a config to a node is driven by the
    %% node_evolve, not the config.
    {reply, ok, State};
handle_call(fetch, _From, State) ->
    {reply, State #state.config, State};
handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

choose_one(N, List) ->
    Len = length(List),
    {lists:nth(1 + (N rem Len), List), N div Len}.

change_shutdown_timeout(Config = #config { shutdown_timeout = X }) ->
    Values = [infinity, 0, 1, 2, 10, 30],
    [Config #config { shutdown_timeout = V } || V <- Values, V =/= X ].
    
change_gospel(Config = #config { gospel = X, nodes = Nodes }) ->
    Values = [reset | [N || {N, _} <- Nodes]],
    [Config #config { gospel = V } || V <- Values, V =/= X ].

add_node_to_cluster(Config = #config { nodes = ConfigNodes }, Namer) ->
    %% We make this lazy so that we don't cause the node_namer to
    %% churn through names
    [fun () ->
             Node = node_namer:get_name(Namer),
             Config #config { nodes = [{Node, disc} | ConfigNodes] }
     end |
     [Config #config { nodes = [{Node, disc} | ConfigNodes] }
      || Node <- node_namer:all_names(Namer),
         not orddict:is_key(Node, ConfigNodes) ]
    ].

remove_node_from_cluster(Config = #config { nodes = ConfigNodes }) ->
    [Config #config { nodes = orddict:erase(N, ConfigNodes) }
     || {N, _} <- ConfigNodes].
