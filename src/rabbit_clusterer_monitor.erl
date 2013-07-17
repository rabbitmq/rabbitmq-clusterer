-module(rabbit_clusterer_monitor).

-behaviour(gen_server).

-export([stop/2]).

-export([start_link/1, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { config,
                 nodes,
                 alive,
                 dead,
                 timer }).

-include("rabbit_clusterer.hrl").

start_link(Config) ->
    {ok, _Pid} = gen_server:start_link(?MODULE, [Config], []).

stop(Pid, Config) ->
    gen_server:cast(Pid, {stop, Config}),
    ok.

init([Config = #config { nodes = Nodes }]) ->
    NodeNames = [N || {N, _} <- Nodes, N =/= node()],
    MRefs = [monitor(process, {rabbit_clusterer_coordinator, N}) ||
                N <- NodeNames],
    {ok, #state { config = Config,
                  nodes  = NodeNames,
                  alive  = MRefs,
                  dead   = [],
                  timer  = undefined }}.

handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast({stop, NewConfig}, State = #state { nodes = Nodes,
                                                alive = Alive }) ->
    [ok = rabbit_clusterer_coordinator:send_new_config(NewConfig, N) ||
        N <- Nodes],
    [demonitor(MRef) || MRef <- Alive],
    {stop, normal, State};
handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info({'DOWN', MRef, process, {rabbit_clusterer_coordinator, Node}, _Info},
            State = #state { dead = Dead, alive = Alive, nodes = Nodes }) ->
    true = lists:member(Node, Nodes), %% ASSERTION
    Alive1 = lists:delete(MRef, Alive),
    Dead1 = [Node | Dead],
    {noreply, ensure_timer(State #state { dead = Dead1, alive = Alive1 })};
handle_info(poke_the_dead, State = #state { dead   = Dead,
                                            alive  = Alive,
                                            config = Config }) ->
    MRefsNew = [monitor(process, {rabbit_clusterer_coordinator, N}) || N <- Dead],
    ok = rabbit_clusterer_coordinator:send_new_config(Config, Dead),
    Alive1 = MRefsNew ++ Alive,
    {noreply, State #state { dead = [], timer = undefined, alive = Alive1 }};
handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

ensure_timer(State = #state { timer = undefined }) ->
    %% TODO: justify 2000
    State #state { timer = erlang:send_after(2000, self(), poke_the_dead) };
ensure_timer(State) ->
    State.

