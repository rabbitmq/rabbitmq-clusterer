-module(rabbit_clusterer_join).

-export([init/3, event/2]).

-record(state, { config, node_id, comms, state }).

-include("rabbit_clusterer.hrl").

init(Config = #config { nodes = Nodes,
                        gospel = Gospel }, NodeID, Comms) ->
    %% 1. Check we're actually involved in this
    case proplists:get_value(node(), Nodes) of
        undefined ->
            %% Oh. We're not in there...
            shutdown;
        disc when length(Nodes) =:= 1 ->
            ok = case Gospel of
                     reset ->
                         rabbit_clusterer_utils:wipe_mnesia(NodeID);
                     {node, _Node} ->
                         %% _utils:proplist_config_to_record ensures
                         %% if we're here, _Node must be =:= node().
                         rabbit_clusterer_utils:eliminate_mnesia_dependencies()
                 end,
            {success, Config};
        ram when length(Nodes) =:= 1 ->
            {error, ram_only_cluster_config};
        _ ->
            request_status(#state { config  = Config,
                                    node_id = NodeID,
                                    comms   = Comms })
    end.

event({comms, {[], _BadNodes}}, State = #state { state = awaiting_status }) ->
    delayed_request_status(State);
event({comms, {Replies, BadNodes}}, State = #state { state  = awaiting_status,
                                                     config = Config }) ->
    {Youngest, OlderThanUs, TransDict} =
        lists:foldr(
          fun ({Node, {ConfigN, TModuleN}}, {YoungestN, OlderThanUsN, TransDictN}) ->
                  {case rabbit_clusterer_utils:compare_configs(ConfigN, YoungestN) of
                       gt -> rabbit_clusterer_utils:merge_node_id_maps(ConfigN, YoungestN);
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
                   dict:append(TModuleN, Node, TransDictN)}
                 end, {Config, [], dict:new()}, Replies),
    case rabbit_clusterer_utils:compare_configs(Youngest, Config) of
        eq ->
            %% We have the most up to date config. Huzzuh. But we must
            %% actually use Youngest from here on as it has the
            %% updated node_id_maps.
            State1 = State #state { config = Youngest },
            case OlderThanUs of
                [] ->
                    case lists:usort(dict:fetch_keys(TransDict)) of
                        [?MODULE] ->
                            %% Everyone who is here is joining. So
                            %% everyone is new to this cluster.
                            case BadNodes of
                                [] ->
                                    %% And everyone who should be here
                                    %% is. So we need to look at
                                    %% gospel, everyone who's not
                                    %% gospel should reset. If no one
                                    %% is gospel then we just elect a
                                    %% leader and go from there.
                                    todo; %% TODO
                                [_|_] ->
                                    %% We have some bad nodes around,
                                    %% so we can't actually trust that
                                    %% everyone really will be new to
                                    %% this cluster. That said, if
                                    %% gospel is a node that is here
                                    %% (or indeed even 'reset') then
                                    %% we can move
                                    %% forwards. Otherwise, we need to
                                    %% wait.
                                    %% TODO
                                    delayed_request_status(State1)
                            end;
                        [ok|_] ->
                            %% Some nodes have actually formed into a
                            %% cluster. We should be able to sync to
                            %% them. We will definitely need to do a
                            %% wipe here.
                            todo;
                        [_|_] ->
                            %% One might think that we should just
                            %% wait and go round again until we see an
                            %% 'ok' and be in the above
                            %% branch. However because host names can
                            %% be reused, there is the possibility
                            %% that previously 'we' were in a cluster,
                            %% 'we' were shut down last, then wiped,
                            %% and are being brought up with the old
                            %% cluster config (but as we were wiped,
                            %% we see it as being new, hence why we're
                            %% in this transitioner). So everyone else
                            %% is rejoining, and is blocked waiting
                            %% for us. The plan is that having sent
                            %% over our (new) node_id in the
                            %% request_status message, the other nodes
                            %% will realise we've been wiped and will
                            %% take appropriate action. Thus here we
                            %% really do just wait to go around again.
                            delayed_request_status(State1)
                    end;
                [_|_] ->
                    %% Update nodes which are older than us. In
                    %% reality they're likely to receive lots of the
                    %% same update from everyone else, but meh,
                    %% they'll just have to cope.
                    update_remote_nodes(OlderThanUs, State1)
            end;
        _ ->
            {config_changed, Youngest}
    end;
event({delayed_request_status, Ref},
      State = #state { state = {delayed_request_status, Ref} }) ->
    request_status(State);
event({request_status, _Ref}, State) ->
    %% ignore it
    {continue, State};
event({node_reset, _Node}, State) ->
    %% I don't think we need to do anything here.
    {continue, State}.


request_status(State = #state { comms   = Comms,
                                node_id = NodeID,
                                config  = #config { nodes = Nodes } }) ->
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
