-module(rabbit_clusterer_utils).

-export([stop_mnesia/0,
         ensure_start_mnesia/0,
         stop_rabbit/0,
         start_rabbit_async/0,
         boot_rabbit_async/0,
         make_mnesia_singleton/1,
         eliminate_mnesia_dependencies/1,
         configure_cluster/2]).

%%----------------------------------------------------------------------------

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
    spawn(fun () -> ok = rabbit:start() end),
    ok.

boot_rabbit_async() ->
    spawn(fun () -> ok = rabbit:boot() end),
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
    ok = ensure_start_mnesia(),
    ok;
make_mnesia_singleton(false) ->
    eliminate_mnesia_dependencies(everyone_else).

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
    NodesToDelete1 =
        case NodesToDelete of
            everyone_else -> mnesia:system_info(db_nodes) -- [node()];
            _             -> NodesToDelete
        end,
    [{atomic,ok} = mnesia:del_table_copy(schema, N) || N <- NodesToDelete1],
    ok = rabbit_node_monitor:reset_cluster_status(),
    ok.

configure_cluster(Nodes, MyNodeType) ->
    case application:load(rabbit) of
        ok                                -> ok;
        {error, {already_loaded, rabbit}} -> ok
    end,
    ok = application:set_env(rabbit, cluster_nodes, {Nodes, MyNodeType}).
