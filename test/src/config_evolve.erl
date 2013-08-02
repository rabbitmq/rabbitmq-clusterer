-module(config_evolve).

-behaviour(gen_server).

-export([observe/2, transmogrify/1, fetch/1]).

-export([start_link/3, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {config, node, node_namer}).

-include("rabbit_clusterer.hrl").

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

handle_call({observe, Seed}, _From, State) ->
    {reply, Seed, State};
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
