-module(node_namer).

-behaviour(gen_server).

-export([get_name/1, all_names/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

get_name(Pid) ->
    gen_server:call(Pid, get_name, infinity).

all_names(Pid) ->
    gen_server:call(Pid, all_names, infinity).

start_link() ->
    {ok, _Pid} = gen_server:start_link(?MODULE, [], []).

init([]) ->
    [$@|Host] = lists:dropwhile(fun (C) -> C =/= $@ end, atom_to_list(node())),
    {ok, {0, Host}}.

handle_call(get_name, _From, {N, Host}) ->
    {reply, list_to_atom(lists:flatten(io_lib:format("node~p@~s", [N, Host]))),
     {N+1, Host}};
handle_call(all_names, _From, {N, Host}) ->
    {reply, 
     [list_to_existing_atom(lists:flatten(io_lib:format("node~p@~s", [M, Host])))
      || M <- lists:seq(0, N-1)],
     {N, Host}};
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
