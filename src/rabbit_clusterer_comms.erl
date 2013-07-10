-module(rabbit_clusterer_comms).

-behaviour(gen_server).

-export([stop/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { token }).

start_link() ->
    Ref = make_ref(),
    {ok, Pid} = gen_server:start_link(?MODULE, [Ref], []),
    {ok, Pid, {Pid, Ref}}.

stop({Pid, _Ref}) ->
    gen_server:cast(Pid, stop),
    ok.

init([Ref]) ->
    {ok, #state { token = {self(), Ref} }}.

handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
