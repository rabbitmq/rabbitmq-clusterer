-module(utils).

-export([choose_one/2, remote_eval/2, rpc_call/4]).

-define(RPC_TIMEOUT, infinity).

choose_one(N, List) ->
    Len = length(List),
    {lists:nth(1 + (N rem Len), List), N div Len}.

remote_eval(Node, ExprStr) ->
    {ok, Scanned, _} = erl_scan:string(ExprStr),
    {ok, Parsed} = erl_parse:parse_exprs(Scanned),
    rpc_call(Node, erl_eval, exprs, [Parsed, []]).

rpc_call(Node, Mod, Fun, Args) ->
    rpc:call(Node, Mod, Fun, Args, ?RPC_TIMEOUT).
