-module(test).

-export([test/0]).

-include("rabbit_clusterer.hrl").

%% {par, [DoAllTheseInParallel]}
%% {seq, [SequenceTheseThings]}

interpret({seq, []}, State) ->
    State;
interpret({seq, [H|T]}, State) ->
    interpret({seq, T}, interpret(H, State));
interpret({par, []}, State) ->
    State;
interpret({par, P}, State) ->
    Ref = make_ref(),
    Self = self(),
    Fun = fun (Result) -> Self ! {Ref, self(), Result}, ok end,
    PidTasks =
        orddict:from_list(
          [{spawn(fun () -> Fun(interpret(T, State)) end), T} || T <- P]),
    merge_results(gather(PidTasks, Ref), State);
interpret({merge_results, Task, TaskState}, State) ->
    %% todo
    State;
interpret(extend, State) ->
    interpret(generate(State));
interpret({exec, Fun}, State) ->
    Fun(State).

gather(PidTasks, Ref) ->
    MRefs = [monitor(process, Pid) || Pid <- orddict:fetch_keys(PidTasks)],
    Results = gather(PidTasks, Ref, orddict:new()),
    [demonitor(MRef, [flush]) || MRef <- MRefs],
    Results.

gather(PidTasks, Ref, Results) ->
    case orddict:size(PidTasks) of
        0 -> Results;
        _ -> receive
                 {Ref, Pid, Result} ->
                     gather(orddict:erase(Pid, PidTasks), Ref,
                            orddict:store(orddict:fetch(Pid, PidTasks),
                                          Result, Results));
                 {'DOWN', _MRef, process, Pid, _Info} ->
                     gather(orddict:erase(Pid, PidTasks), Ref,
                            orddict:update(orddict:fetch(Pid, PidTasks),
                                           fun id/1, failed, Results))
             end
    end.

merge_results(TaskResults, State) ->
    interpret(
      {seq, [{merge, T, R} || {T, R} <- orddict:to_list(TaskResults)]}, State).

id(X) -> X.

test() ->
    ok.
