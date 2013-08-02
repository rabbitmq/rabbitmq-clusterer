-module(node_evolve).

-behaviour(gen_server).

-export([observe/2, transmogrify/1, verify/1]).

-export([start_link/2, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {node,
                config_evolve,
                node_namer,
                observed_config,
                observed_status,
                predicted_status
                action
               }).

-include("rabbit_clusterer.hrl").

-define(IS_BENIGN_EXIT(R),
        R =:= noproc; R =:= noconnection; R =:= nodedown; R =:= normal;
            R =:= shutdown).

observe(Pid, Seed) ->
    gen_server:call(Pid, {observe, Seed}, infinity).

transmogrify(Pid) ->
    gen_server:call(Pid, transmogrify, infinity).

verify(Pid) ->
    gen_server:call(Pid, verify, infinity).

start_link(Node, Namer) ->
    {ok, _Pid} = gen_server:start_link(?MODULE, [Node, Namer], []).

init([Node, Namer]) ->
    {ok, #state { node = Node, node_namer = Namer }}.

handle_call({observe, Seed}, _From, State = #state { node = Node }) ->
    Response = request_status(Node),
    Config = case Response of
                 {ok, {Config1 = #config {}, _Status}} -> Config1;
                 _                                     -> undefined
             end,
    Status = case Response of
                 {ok, {#config {}, Status1}}                        -> Status1;
                 {ok, preboot}                                      -> preboot;
                 {error, exit, {R, _}}      when ?IS_BENIGN_EXIT(R) -> nodedown;
                 {error, exit, {{R, _}, _}} when ?IS_BENIGN_EXIT(R) -> nodedown;
                 _                                                  -> undefined
             end,
    State1 = ensure_config_envolver(State #state { observed_config = Config,
                                                   observed_status = Status }),
    {Seed1, State2} = pick_action(Seed, State1),
    Seed2 = maybe_config_observe(Seed1, State2),
    {reply, Seed2, State2};
handle_call(transmogrify, _From, State) ->
    {reply, ok, State};
handle_call(verify, _From, State) ->
    {reply, ok, State};
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


ensure_config_envolver(State = #state { config_evolve   = undefined,
                                        observed_config = Config = #config {},
                                        node            = Node,
                                        node_namer      = Namer }) ->
    {ok, Pid} = config_evolve:start_link(Node, Config, Namer),
    State #state { config_evolve = Pid };
ensure_config_envolver(State) ->
    State.

request_status(Node) ->
    try
        {ok, gen_server:call({rabbit_clusterer_coordinator, Node},
                             {request_status, node(), <<>>}, infinity)}
    catch
        Class:Error ->
            {error, Class, Error}
    end.

maybe_config_observe(Seed, #state { config_evolve = undefined }) ->
    Seed;
maybe_config_observe(Seed, #state { config_evolve = Pid }) ->
    config_evolve:observed_config(Pid, Seed).

pick_action(0, State) ->
    {0, State #state { action = no_action }};
pick_action(N, State) ->
    todo.
