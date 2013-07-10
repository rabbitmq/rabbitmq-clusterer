-module(rabbit_clusterer_comms).

-behaviour(gen_server).

-export([stop/1, multi_call/3]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { token }).

-define(TARGET, rabbit_clusterer_coordinator).

start_link() ->
    Ref = make_ref(),
    {ok, Pid} = gen_server:start_link(?MODULE, [Ref], []),
    {ok, Pid, {Pid, Ref}}.

stop({Pid, _Ref}) ->
    gen_server:cast(Pid, stop),
    ok.

multi_call(Nodes, Msg, {Pid, _Ref}) ->
    %% We do a cast, not a call, so that the caller doesn't block -
    %% the result gets sent back async. This is essential to avoid a
    %% potential deadlock.
    gen_server:cast(Pid, {multi_call, self(), Nodes, Msg}),
    ok.

init([Ref]) ->
    {ok, #state { token = {self(), Ref} }}.

handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast({multi_call, ReplyTo, Nodes, Msg},
            State = #state { token = Token }) ->
    Result = gen_server:multi_call(Nodes, ?TARGET, Msg),
    ReplyTo ! {comms, Token, Result},
    {noreply, State};
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
