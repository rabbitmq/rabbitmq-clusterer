-module(rabbit_clusterer_utils).

-include("rabbit_clusterer.hrl").

-export([stop_mnesia/0,
         ensure_start_mnesia/0,
         stop_rabbit/0,
         start_rabbit_async/0,
         boot_rabbit_async/0,
         wipe_mnesia/0,
         eliminate_mnesia_dependencies/1,
         configure_cluster/1,
         analyse_node_statuses/3
        ]).

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

wipe_mnesia() ->
    %% With mnesia not running, we can't call
    %% rabbit_mnesia:force_reset() because that tries to read in the
    %% cluster status files from the mnesia directory which might not
    %% exist if we're a completely virgin node. So we just do the rest
    %% manually.
    error_logger:info_msg("Clusterer Resetting Rabbit~n"),
    ok = rabbit_file:recursive_delete(
           filelib:wildcard(rabbit_mnesia:dir() ++ "/*")),
    ok = rabbit_node_monitor:reset_cluster_status(),
    ok.

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
    ok.

configure_cluster(NodeDict) ->
    case application:load(rabbit) of
        ok                                -> ok;
        {error, {already_loaded, rabbit}} -> ok
    end,
    NodeNames = orddict:fetch_keys(NodeDict),
    Mode = orddict:fetch(node(), NodeDict),
    ok = application:set_env(rabbit, cluster_nodes, {NodeNames, Mode}).

%% The input is a k/v list of nodes and their config+status tuples (or
%% the atom 'preboot' if the node is in the process of starting up),
%% plus the local node's config and id.
%%
%% Returns a tuple containing
%% 1) the youngest config of all,
%% 2) a list of nodes operating with configs older than the local node's
%% 3) a dict mapping status to lists of nodes
analyse_node_statuses(NodeConfigStatusList, Config, NodeID) ->
    Analysis =
        lists:foldr(
          fun (_Reply, invalid) ->
                  invalid;
              ({N, preboot}, {YoungestN, OlderN, IDsN, StatusesN}) ->
                  {YoungestN, OlderN, IDsN, dict:append(preboot, N, StatusesN)};
              ({N, {ConfigN, StatusN}}, {YoungestN, OlderN, IDsN, StatusesN}) ->
                  VsYoung = rabbit_clusterer_config:compare(ConfigN, YoungestN),
                  VsConfig = rabbit_clusterer_config:compare(ConfigN, Config),
                  case VsYoung =:= invalid orelse VsConfig =:= invalid of
                      true ->
                          invalid;
                      false ->
                          YoungestN1 = case VsYoung of
                                           gt -> ConfigN;
                                           _  -> YoungestN
                                       end,
                          OlderN1 = case VsConfig of
                                        lt -> [N | OlderN];
                                        _  -> OlderN
                                    end,
                          NodeIDN =
                              orddict:fetch(N, ConfigN #config.map_node_id),
                          IDsN1 = [{N, NodeIDN} | IDsN],
                          StatusesN1 = dict:append(StatusN, N, StatusesN),
                          {YoungestN1, OlderN1, IDsN1, StatusesN1}
                  end
          end, {Config, [], [], dict:new()}, NodeConfigStatusList),
    case Analysis of
        invalid ->
            invalid;
        {Youngest, Older, IDs, Status} ->
            %% We want to make sure anything that we had in Config
            %% that does not exist in IDs is still maintained.
            Youngest1 =
                rabbit_clusterer_config:merge(
                  NodeID,
                  Youngest #config { map_node_id = orddict:from_list(IDs) },
                  Config),
            {Youngest1, Older, Status}
    end.
