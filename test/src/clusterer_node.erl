-module(clusterer_node).

-export([observe_state/1, all_stable/1,
         start_link/1, delete/1,
         reset/1, start/1, start_with_config/2, apply_config/2, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

observe_state(Pids) ->
    todo.

all_stable(Pids) ->
    todo.

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

start_link(Name) ->
    gen_server:start_link(?MODULE, [Name], []).

delete(Pid) -> gen_server:cast(Pid, delete).

reset(Pid) -> gen_server:cast(Pid, reset).

start(Pid) -> gen_server:cast(Pid, start).

start_with_config(Pid, Config) ->
    gen_server:cast(Pid, {start_with_config, Config}).

apply_config(Pid, Config) -> gen_server:cast(Pid, {apply_config, Config}).

stop(Pid) -> gen_server:cast(Pid, stop).

%% >=---=<80808080808>=---|v|v|---=<80808080808>=---=<

init([Name]) ->
    {ok, Name}.

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
