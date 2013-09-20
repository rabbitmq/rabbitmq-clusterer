-module(rabbit_clusterer_utils).

-export([stop_mnesia/0,
         stop_rabbit/0,
         start_rabbit_async/1,
         boot_rabbit_async/1,
         make_mnesia_singleton/1,
         eliminate_mnesia_dependencies/1,
         configure_cluster/2]).

%%----------------------------------------------------------------------------

-define(PRE_SLEEP, 10000). %% 10 seconds

stop_mnesia() ->
    stopped = mnesia:stop(),
    ok.

ensure_start_mnesia() ->
    ok = mnesia:start().

stop_rabbit() ->
    case application:stop(rabbit) of
        ok                             -> ok;
        {error, {not_started, rabbit}} -> ok;
        Other                          -> Other
    end.

start_rabbit_async(PreSleep) ->
    ok = spawn_starter(fun rabbit:start/0, PreSleep).

boot_rabbit_async(PreSleep) ->
    ok = spawn_starter(fun rabbit:boot/0, PreSleep).

spawn_starter(Fun, PreSleep) ->
    spawn(fun () ->
                  case PreSleep of
                      true  -> timer:sleep(?PRE_SLEEP);
                      false -> ok
                  end,
                  try
                      ok = Fun()
                  catch
                      _Class:_Reason ->
                          rabbit_clusterer_coordinator:rabbit_boot_failed()
                  end
          end),
    ok.

make_mnesia_singleton(true) ->
    %% With mnesia not running, we can't call
    %% rabbit_mnesia:force_reset() because that tries to read in the
    %% cluster status files from the mnesia directory which might not
    %% exist if we're a completely virgin node. So we just do the rest
    %% manually.
    error_logger:info_msg("Clusterer Resetting Rabbit~n"),
    ok = rabbit_mnesia:ensure_mnesia_dir(),
    ok = rabbit_file:recursive_delete(
           filelib:wildcard(rabbit_mnesia:dir() ++ "/*")),
    ok = rabbit_node_monitor:reset_cluster_status(),
    ok;
make_mnesia_singleton(false) ->
    %% Note that this is wrong: in this case we actually want to
    %% eliminate everyone who isn't in our cluster - i.e. everyone
    %% mnesia thinks we're currently clustered with. However, due to
    %% limitations with del_table_copy (i.e. mnesia must not be
    %% running on remote node; it must be running on our node), this
    %% is difficult to orchestrate: it's easiest done by the
    %% eliminated nodes doing an RPC to us. But there are still cases
    %% where that may not work out correctly. However, this scenario
    %% can only occur when a cluster is being split up into other
    %% clusters. For MVP and this project, we don't consider that a
    %% use case, so we're going to just ignore this problem for the
    %% time being.
    eliminate_mnesia_dependencies([]).

eliminate_mnesia_dependencies(NodesToDelete) ->
    ok = rabbit_mnesia:ensure_mnesia_dir(),
    ok = ensure_start_mnesia(),
    %% rabbit_table:force_load() does not error if
    %% mnesia:force_load_table errors(!) Thus we can safely run this
    %% even in clean state - i.e. one where neither the schema nor any
    %% tables actually exist.
    ok = rabbit_table:force_load(),
    case rabbit_table:is_present() of
        true  -> ok = rabbit_table:wait_for_replicated();
        false -> ok
    end,
    %% del_table_copy has to be done after the force_load but is also
    %% usefully idempotent.
    [{atomic,ok} = mnesia:del_table_copy(schema, N) || N <- NodesToDelete],
    ok = rabbit_node_monitor:reset_cluster_status(),
    ok = stop_mnesia(),
    ok.

configure_cluster(Nodes, MyNodeType) ->
    case application:load(rabbit) of
        ok                                -> ok;
        {error, {already_loaded, rabbit}} -> ok
    end,
    ok = application:set_env(rabbit, cluster_nodes, {Nodes, MyNodeType}).
