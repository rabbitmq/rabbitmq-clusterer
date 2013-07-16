-module(rabbit_clusterer_join).

-export([init/2, event/2]).

-record(state, { config, comms, state }).

-include("rabbit_clusterer.hrl").

init(Config = #config { nodes = Nodes,
                        gospel = Gospel }, Comms) ->
    %% 1. Check we're actually involved in this
    case proplists:get_value(node(), Nodes) of
        undefined ->
            %% Oh. We're not in there...
            shutdown;
        disc when length(Nodes) =:= 1 ->
            ok = case Gospel of
                     reset ->
                         rabbit_clusterer_utils:wipe_mnesia();
                     {node, _Node} ->
                         %% _utils:proplist_config_to_record ensures
                         %% if we're here, _Node must be =:= node().
                         rabbit_clusterer_utils:eliminate_mnesia_dependencies()
                 end,
            {success, Config};
        ram when length(Nodes) =:= 1 ->
            {error, ram_only_cluster_config};
        _ ->
            request_status(#state { config = Config,
                                    comms  = Comms })
    end.

event({comms, {[], _BadNodes}}, State = #state { state = awaiting_status }) ->
    delayed_request_status(State);
event({comms, {Replies, BadNodes}}, State = #state { state  = awaiting_status,
                                                     config = Config }) ->
    {Youngest, OlderThanUs, StatusDict} =
        lists:foldr(
          fun ({Node, preboot}, {YoungestN, OlderThanUsN, StatusDictN}) ->
                  {YoungestN, OlderThanUsN, dict:append(preboot, Node, StatusDictN)};
              ({Node, {ConfigN, StatusN}}, {YoungestN, OlderThanUsN, StatusDictN}) ->
                  {case rabbit_clusterer_utils:compare_configs(ConfigN, YoungestN) of
                       gt -> rabbit_clusterer_utils:merge_configs(ConfigN, YoungestN);
                       invalid ->
                           %% TODO tidy this up - probably shouldn't be a throw.
                           throw("Configs with same version numbers but semantically different");
                       _  -> YoungestN
                   end,
                   case rabbit_clusterer_utils:compare_configs(ConfigN, Config) of
                       lt -> [Node | OlderThanUsN];
                       invalid ->
                           %% TODO tidy this up - probably shouldn't be a throw.
                           throw("Configs with same version numbers but semantically different");
                       _  -> OlderThanUsN
                   end,
                   dict:append(StatusN, Node, StatusDictN)}
          end, {Config, [], dict:new()}, Replies),
    %% Expected keys in StatusDict are:
    %% - preboot:
    %%    clusterer has started, but the boot step not yet hit
    %% - {transitioner, rabbit_clusterer_join} (i.e. ?MODULE):
    %%    it's joining some cluster - blocked in clusterer
    %% - {transitioner, rabbit_clusterer_rejoin}:
    %%    it's rejoining some cluster - blocked in clusterer
    %% - booting:
    %%    clusterer is happy and the rest of rabbit is currently booting
    %% - ready:
    %%    clusterer is happy and enough of rabbit has booted
    %% - pending_shutdown:
    %%    clusterer is waiting for the shutdown timeout and will then exit
    case rabbit_clusterer_utils:compare_configs(Youngest, Config) of
        eq ->
            %% We have the most up to date config. But we must use
            %% Youngest from here on as it has the updated
            %% node_id_maps.
            State1 = State #state { config = Youngest },
            #config { nodes = Nodes, gospel = Gospel } = Youngest,
            case OlderThanUs of
                [_|_] ->
                    %% Update nodes which are older than us. In
                    %% reality they're likely to receive lots of the
                    %% same update from everyone else, but meh,
                    %% they'll just have to cope.
                    update_remote_nodes(OlderThanUs, State1);
                [] ->
                    %% Everyone here has the same config
                    ReadyNodes = case dict:find(ready, StatusDict) of
                                     {ok, List} -> List;
                                     error      -> []
                                 end,
                    AllJoining = [{transitioner, ?MODULE}] =:= dict:fetch_keys(StatusDict),
                    %% ReadyNodes are nodes that are in this cluster
                    %% (well, they could be in any cluster, but seeing
                    %% as we've checked everyone has the same cluster
                    %% config as us, we're sure it really is *this*
                    %% cluster) and are fully up and running.
                    %%
                    %% If ReadyNodes exists, we should just reset and
                    %% join into that, and ignore anything about
                    %% gospel: it's possible that gosple is {node,
                    %% node()} but that, in combination with
                    %% ReadyNodes, suggests that the cluster
                    %% previously existed with an older version of
                    %% 'us': we must have been cleaned out and
                    %% restarted (aka host-name reuse). Here we don't
                    %% care about BadNodes.
                    %%
                    %% If ReadyNodes doesn't exist we can only safely
                    %% proceed if there are no BadNodes, and everyone
                    %% is joining (rather than rejoining)
                    %% i.e. transition module for all is ?MODULE. In
                    %% all other cases, we must wait:
                    %%
                    %% - If BadNodes =/= [] then there may be a node
                    %%   that was cleanly shutdown last with what we
                    %%   think is the current config and so if it was
                    %%   started up, it would rejoin (itself, sort of)
                    %%   and then become ready: we could then sync to
                    %%   it.
                    %%
                    %% - If the transition module is not all ?MODULE
                    %%   then some other nodes must be rejoining. We
                    %%   should wait for them to succeed (or at least
                    %%   change state) because if they do succeed we
                    %%   should sync off them.
                    case ReadyNodes of
                        [_|_] ->
                            ok = rabbit_clusterer_utils:wipe_mnesia(),
                            ok = rabbit_clusterer_utils:configure_cluster(Nodes),
                            {success, Youngest};
                        [] when AllJoining andalso BadNodes =:= [] ->
                            {Wipe, Leader} =
                                case Gospel of
                                    {node, Node} ->
                                        {Node =/= node(), Node};
                                    reset ->
                                        [Leader1 | _] =
                                            lists:usort([N || {N, disc} <- Nodes]),
                                        {true, Leader1}
                                end,
                            case node() of
                                Leader ->
                                    ok = case Wipe of
                                             true  -> rabbit_clusterer_utils:wipe_mnesia();
                                             false -> rabbit_clusterer_utils:eliminate_mnesia_dependencies()
                                         end,
                                    ok = case node() of
                                             Leader -> ok;
                                             _      -> rabbit_clusterer_utils:configure_cluster(Nodes)
                                         end,
                                    {success, Youngest};
                                _ ->
                                    delayed_request_status(State1)
                            end;
                        [] ->
                            delayed_request_status(State1)
                    end
            end;
        _ ->
            {config_changed, Youngest}
    end;
event({delayed_request_status, Ref},
      State = #state { state = {delayed_request_status, Ref} }) ->
    request_status(State);
event({delayed_request_status, _Ref}, State) ->
    %% ignore it
    {configure, State};
event({node_reset, _Node}, State) ->
    %% I don't think we need to do anything here.
    {continue, State}.


request_status(State = #state { comms  = Comms,
                                config = #config { nodes   = Nodes,
                                                   node_id = NodeID } }) ->
    NodesNotUs = [ N || {N, _Mode} <- Nodes, N =/= node() ],
    ok = rabbit_clusterer_comms:multi_call(
           NodesNotUs, {request_status, node(), NodeID}, Comms),
    {continue, State #state { state = awaiting_status }}.

delayed_request_status(State) ->
    %% TODO: work out some sensible timeout value
    Ref = make_ref(),
    {sleep, 1000, {delayed_request_status, Ref},
     State #state { state = {delayed_request_status, Ref} }}.

update_remote_nodes(Nodes, State = #state { config = Config, comms = Comms }) ->
    %% Assumption here is Nodes does not contain node(). We
    %% deliberately do this cast out of Comms to preserve ordering of
    %% messages.
    ok = rabbit_clusterer_comms:multi_cast(Nodes, {new_config, Config}, Comms),
    request_status(State).
