-module(rabbit_clusterer_comms).

-behaviour(gen_server).

-export([stop/1, multi_call/3, multi_cast/3, unlock/2, lock/2, lock_nodes/2]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, { token, locked_by, locking }).

-define(TARGET, rabbit_clusterer_coordinator).

%% In general the comms process exists to perform blocking calls to
%% other nodes, without causing the main coordinator process to
%% block. Thus the communication between the coordinator and the comms
%% is always async even if the comms process goes on to do blocking
%% communication with other nodes. Thus we explain the existence of
%% multi_call.
%%
%% Once we have multi_call and we care about message arrival order, we
%% have to have multi_cast too so that messages arrive in the same
%% order they were sent.
%%
%% We also push the locking API in here. This is rather more complex
%% and is only used by the rejoin transitioner, where it is also
%% documented. But essentially the comms pid is the lock, and the lock
%% is taken by some other pid, which the lock monitors. Should the pid
%% that holds the lock die, the lock is released.

start_link() ->
    Ref = make_ref(),
    {ok, Pid} = gen_server:start_link(?MODULE, [Ref], []),
    {ok, Pid, {Pid, Ref}}.

stop({Pid, _Ref}) ->
    gen_server:cast(Pid, stop).

multi_call(Nodes, Msg, {Pid, _Ref}) ->
    %% We do a cast, not a call, so that the caller doesn't block -
    %% the result gets sent back async. This is essential to avoid a
    %% potential deadlock.
    gen_server:cast(Pid, {multi_call, self(), Nodes, Msg}).

multi_cast(Nodes, Msg, {Pid, _Ref}) ->
    %% Reason for doing this is to ensure that both abcasts and
    %% multi_calls originate from the same process and so will be
    %% received in the same order as they're sent.
    gen_server:cast(Pid, {multi_cast, Nodes, Msg}).

%% public api
lock_nodes(Nodes = [_|_], {Pid, _Ref}) ->
    gen_server:cast(Pid, {lock_nodes, self(), Nodes}).

%% passed through from coordinator
lock(Locker, {Pid, _Ref}) ->
    gen_server:cast(Pid, {lock, Locker}).

%% passed through from coordinator
unlock(Locker, {Pid, _Ref}) ->
    gen_server:cast(Pid, {unlock, Locker}).

%%----------------------------------------------------------------------------

init([Ref]) ->
    {ok, #state { token     = {self(), Ref},
                  locked_by = undefined,
                  locking   = undefined }}.

handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast({multi_call, ReplyTo, Nodes, Msg},
            State = #state { token = Token }) ->
    %% 'infinity' does not cause it to wait for badnodes to become
    %% good.
    Result = gen_server:multi_call(Nodes, ?TARGET, Msg, infinity),
    gen_server:cast(ReplyTo, {comms, Token, Result}),
    {noreply, State};

handle_cast({multi_cast, Nodes, Msg}, State) ->
    abcast = gen_server:abcast(Nodes, ?TARGET, Msg),
    {noreply, State};

handle_cast({lock_nodes, ReplyTo, Nodes},
            State = #state { locking = undefined }) ->
    true = lists:member(node(), Nodes), %% ASSERTION
    %% Of course, all of this has to be async too...
    [First|_] = SortedNodes = lists:usort(Nodes),
    [monitor(process, {?TARGET, N}) || N <- SortedNodes],
    gen_server:cast({?TARGET, First}, {lock, self()}),
    {noreply, State #state { locking = {[], SortedNodes, ReplyTo} }};

handle_cast({lock_ok, Node},
            State = #state { locking = {_Locked, [Node], ReplyTo},
                             token   = Token }) ->
    gen_server:cast(ReplyTo, {comms, Token, lock_ok}),
    {noreply, State #state { locking = undefined }};
handle_cast({lock_ok, Node},
            State = #state { locking = {Locked, [Node,Next|ToLock], ReplyTo} }) ->
    gen_server:cast({?TARGET, Next}, {lock, self()}),
    {noreply, State #state { locking = {[Node|Locked], [Next|ToLock], ReplyTo} }};

handle_cast({lock_rejected, Node},
            State = #state { locking = {Locked, [Node|_ToLock], ReplyTo},
                             token   = Token }) ->
    gen_server:cast(ReplyTo, {comms, Token, lock_rejected}),
    abcast = gen_server:abcast(Locked, ?TARGET, {unlock, self()}),
    {noreply, State #state { locking = undefined }};

handle_cast({lock, Locker}, State = #state { locked_by = undefined }) ->
    gen_server:cast(Locker, {lock_ok, node()}),
    monitor(process, Locker),
    {noreply, State #state { locked_by = Locker }};
handle_cast({lock, Locker}, State) ->
    gen_server:cast(Locker, {lock_rejected, node()}),
    {noreply, State};

handle_cast({unlock, Locker}, State = #state { locked_by = Locker }) ->
    {noreply, State #state { locked_by = undefined }};
handle_cast({unlock, _Locker}, State) ->
    %% Potential race between the DOWN and the unlock might well mean
    %% that the DOWN gets here first, thus we unlock ourselves. At
    %% that point we're free to be locked by someone else. Later on,
    %% the unlock from the DOWN'd process gets here. Thus we don't
    %% attempt to make any assertions about only receiving an unlock
    %% from X when locked by X. Also, this could be an unlock coming
    %% from a remote node which was originally for a lock held by an
    %% older comms which has since been killed off.
    {noreply, State};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.


handle_info({'DOWN', _MRef, process, {?TARGET, Node}, _Info},
            State = #state { locking = Locking, token = Token }) ->
    %% This DOWN must be from some node we're trying to lock.
    Locking1 = case Locking of
                   undefined ->
                       Locking;
                   {_Locked, [Node], ReplyTo} ->
                       gen_server:cast(ReplyTo, {comms, Token, lock_ok}),
                       undefined;
                   {Locked, [Node,Next|ToLock], ReplyTo} ->
                       gen_server:cast({?TARGET, Next}, {lock, self()}),
                       {[Node|Locked], [Next|ToLock], ReplyTo};
                   {Locked, ToLock, ReplyTo} ->
                       {Locked -- [Node], ToLock -- [Node], ReplyTo}
               end,
    {noreply, State # state { locking = Locking1 }};
handle_info({'DOWN', _MRef, process, Pid, _Info},
            State = #state { locked_by = Pid }) ->
    {noreply, State #state { locked_by = undefined }};
handle_info({'DOWN', _MRef, process, _Pid, _Info}, State) ->
    {noreply, State};
handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
