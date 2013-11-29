-module(rabbit_clusterer_utils).

-export([stop_mnesia/0,
         stop_rabbit/0,
         start_rabbit_async/0,
         boot_rabbit_async/0,
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

start_rabbit_async() ->
    ok = spawn_starter(fun rabbit:start/0).

boot_rabbit_async() ->
    ok = spawn_starter(fun rabbit:boot/0).

spawn_starter(Fun) ->
    spawn(fun () ->
                  try
                      ok = Fun(),
                      rabbit_clusterer_coordinator:rabbit_booted()
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
    ok = remove_from_cluster_status(NodesToDelete),
    ok = stop_mnesia(),
    %% We had to force load in case we had to delete any schemas. But
    %% once we've stopped mnesia (and we have to because rabbit
    %% upgrades expect to find mnesia stopped), mnesia seems to forget
    %% that it's been force_loaded and thus should now really behave
    %% as if it's the master. Consequently we have to touch the
    %% force_load file in the mnesia dir which rabbit_mnesia then
    %% finds and does another force load when rabbit actually boots.
    ok = rabbit_file:write_file(filename:join(rabbit_mnesia:dir(), "force_load"), <<"">>),
    ok.

configure_cluster(Nodes, MyNodeType) ->
    case application:load(rabbit) of
        ok                                -> ok;
        {error, {already_loaded, rabbit}} -> ok
    end,
    ok = application:set_env(rabbit, cluster_nodes, {Nodes, MyNodeType}).

remove_from_cluster_status(Nodes) ->
    try
        {All, Disc, Running} = rabbit_node_monitor:read_cluster_status(),
        ok = rabbit_node_monitor:write_cluster_status(
               {All -- Nodes, Disc -- Nodes, Running -- Nodes})
    catch
        {error, {corrupt_or_missing_cluster_files, _Stat, _Run}} ->
            ok = rabbit_node_monitor:reset_cluster_status()
    end.
