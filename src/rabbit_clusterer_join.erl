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
    {Youngest, OlderThanUs, TransDict} =
        lists:foldr(
          fun ({Node, {ConfigN, TModuleN}}, {YoungestN, OlderThanUsN, TransDictN}) ->
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
                   dict:append(TModuleN, Node, TransDictN)}
                 end, {Config, [], dict:new()}, Replies),
    case rabbit_clusterer_utils:compare_configs(Youngest, Config) of
        eq ->
            %% We have the most up to date config. But we must use
            %% Youngest from here on as it has the updated
            %% node_id_maps.
            State1 = State #state { config = Youngest },
            #config { nodes = Nodes, gospel = Gospel } = Youngest,
            case OlderThanUs of
                [] ->
                    %% Everyone here has the same config
                    {Wipe, Leader} =
                        case Gospel of
                            {node, Node} ->
                                {Node =/= node(), Node};
                            reset ->
                                [Leader1 | _] =
                                    lists:usort([N || {N, disc} <- Nodes]),
                                {true, Leader1}
                        end,
                    %% ASSERTION: Be sure that we know about Leader somehow
                    true = Leader =:= node() orelse
                        lists:member(Leader, BadNodes) orelse
                        lists:keymember(Leader, 1, Replies),
                    ReadyNodes = case dict:find(ready, TransDict) of
                                     {ok, List} -> List;
                                     error      -> []
                                 end,
                    AwaitingLeader = Leader =/= node()
                        andalso (lists:member(Leader, BadNodes)
                                 orelse not lists:member(Leader, ReadyNodes)),
                    case AwaitingLeader of
                        true ->
                            delayed_request_status(State1);
                        false ->
                            ok = case Wipe of
                                     true  -> rabbit_clusterer_utils:wipe_mnesia();
                                     false -> rabbit_clusterer_utils:eliminate_mnesia_dependencies()
                                 end,
                            ok = case node() of
                                     Leader -> ok;
                                     _      -> rabbit_clusterer_utils:configure_cluster(Nodes)
                                 end,
                            {success, Youngest}
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
