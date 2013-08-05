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
                predicted_status,
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
    Inspection = inspect_node(Node),
    case validate_situation(Node, Inspection) of
        true ->
            State1 = case Inspection of
                         {Config = #config {}, Status, _Running} ->
                             State #state { observed_config = Config,
                                            observed_status = Status };
                         {Config, Status} ->
                             State #state { observed_config = Config,
                                            observed_status = Status }
                     end,
            State2 = ensure_config_evolver(State1),
            {Seed1, State3} = pick_action(Seed, State2),
            Seed2 = maybe_config_observe(Seed1, State3),
            {reply, Seed2, State3};
        false ->
            {stop, normal, validation_error, State}
    end;
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


ensure_config_evolver(State = #state { config_evolve   = undefined,
                                       observed_config = Config = #config {},
                                       node            = Node,
                                       node_namer      = Namer }) ->
    {ok, Pid} = config_evolve:start_link(Node, Config, Namer),
    State #state { config_evolve = Pid };
ensure_config_evolver(State) ->
    State.

inspect_node(Node) ->
    Result1 = request_status(Node),
    Running = stuff_running_on(Node),
    Result2 = request_status(Node),
    %% The logic here is that if both Results have the same Config,
    %% then you can treat Running as being an atomic call with both of
    %% those. Ish.
    case {Result1, Result2} of
        {{Config = #config {}, _Status1}, {Config, Status2}} ->
            {Config, Status2, Running};
        _ ->
            Result1
    end.

request_status(Node) ->
    case
        try
            {ok, gen_server:call({rabbit_clusterer_coordinator, Node},
                                 {request_status, node(), <<>>}, infinity)}
        catch
            Class:Error ->
                {error, Class, Error}
        end of
        {ok, {#config {} = Config, Status}}                -> {Config, Status};
        {ok, preboot}                                      -> {undefined, preboot};
        {error, exit, {R, _}} when ?IS_BENIGN_EXIT(R)      -> {undefined, nodedown};
        {error, exit, {{R, _}, _}} when ?IS_BENIGN_EXIT(R) -> {undefined, nodedown}
    end.

maybe_config_observe(Seed, #state { config_evolve = undefined }) ->
    Seed;
maybe_config_observe(Seed, #state { config_evolve = Pid }) ->
    config_evolve:observed_config(Pid, Seed).

pick_action(0, State) ->
    {0, State #state { action = no_action }};
pick_action(N, State #state { observed_status = nodedown, predicted_status = nodedown }) ->
    todo.

stuff_running_on(Node) ->
    case utils:remote_eval(Node, "application:which_applications().") of
        {badrpc, _} ->
            [{clusterer, false}, {mnesia, false}, {rabbit, false}];
        {value, Results, _} ->
            [{clusterer, [] =/= [true || {rabbitmq_clusterer,_,_} <- Results ]},
             {mnesia, [] =/= [true || {mnesia,_,_} <- Results]},
             {rabbit, [] =/= [true || {rabbit,_,_} <- Results]}]
    end.

validate_situation(_Node, {undefined, nodedown}) ->
    true;
validate_situation(_Node, {undefined, preboot}) ->
    true;
validate_situation(Node, {#config { nodes = Nodes }, pending_shutdown}) ->
    not orddict:is_key(Node, Nodes);
validate_situation(Node, {#config { nodes = Nodes }, booting}) ->
    orddict:is_key(Node, Nodes);
validate_situation(Node, {#config { nodes = Nodes }, ready}) ->
    orddict:is_key(Node, Nodes);
validate_situation(_Node, {#config {}, {transitioner, _}}) ->
    true;

validate_situation(_Node, {_Config, {transitioner, _}, Running}) ->
    orddict:fetch(clusterer, Running);
validate_situation(Node, {#config { nodes = Nodes }, booting, Running}) ->
    %% If we were 'booting' both sides of stuff_running_on then rabbit
    %% cannot be started.
    orddict:fetch(clusterer, Running) andalso
        (not orddict:fetch(rabbit, Running)) andalso
        orddict:is_key(Node, Nodes);
validate_situation(Node, {#config { nodes = Nodes }, ready, Running}) ->
    %% Sadly with ready, you can't quite infer that the rabbit
    %% application is actually started - which_applications doesn't
    %% report an application until it's fully started, which may occur
    %% later than clusterer reports ready.
    orddict:fetch(clusterer, Running) andalso 
        orddict:fetch(mnesia, Running) andalso 
        orddict:is_key(Node, Nodes);
validate_situation(Node, {#config { nodes = Nodes }, pending_shutdown, Running}) ->
    orddict:fetch(clusterer, Running) andalso
        (not orddict:fetch(mnesia, Running)) andalso 
        (not orddict:fetch(rabbit, Running)) andalso 
        (not orddict:is_key(Node, Nodes)).
