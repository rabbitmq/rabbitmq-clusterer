-module(rabbit_clusterer_join).

-export([init/3, event/2]).

-record(state, { config, node_id, comms, state }).

-include("rabbit_clusterer.hrl").

init(Config = #config { nodes = Nodes }, NodeID, Comms) ->
    %% 1. Check we're actually involved in this
    case proplists:get_value(node(), Nodes) of
        undefined ->
            %% Oh. We're not in there...
            shutdown;
        disc when length(Nodes) =:= 1 ->
            %% Simple: we're just clustering with ourself and we're
            %% disk. Just do a wipe and we're done.
            ok = rabbit_clusterer_utils:wipe_mnesia(NodeID),
            {success, Config};
        ram when length(Nodes) =:= 1 ->
            {error, ram_only_cluster_config};
        _ ->
            request_status(#state { config  = Config,
                                    node_id = NodeID,
                                    comms   = Comms })
    end.

%% Strategy:
%% 1. If there are only BadNodes then we need to wait for a bit
%%   and then multi_call again.
%% 2. If our own config is not the youngest then we need to grab
%%   the youngest and start again.
%% 3. If your own config is younger than anyone else then we need
%%   to apply our own config to them (and continue).
%% 4. We then wait for either someone to be active, or for
%%   everyone to be inactive but no BadNodes. In either case, we
%%   can then actually start.
event({comms, {[], _BadNodes}}, State = #state { state = awaiting_status }) ->
    delayed_request_status(State);
event({comms, {Replies, BadNodes}}, State = #state { state  = awaiting_status,
                                                     config = Config }) ->
    {Youngest, OlderThanUs, TransDict} =
        lists:foldr(
          fun ({Node, {ConfigN, TModuleN}}, {YoungestN, OlderThanUsN, TransDictN}) ->
                  {case rabbit_clusterer_utils:compare_configs(ConfigN, YoungestN) of
                       gt -> ConfigN;
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
    case Youngest of
        Config ->
            %% We have the most up to date config. Huzzuh.
            case {BadNodes, OlderThanUs, dict:to_list(TransDict)} of
                {[], [], [{?MODULE, _}]} ->
                    %% Everyone is here, everyone has our config,
                    %% everyone is new.  We can make an executive
                    %% decision as to who should lead and just go for
                    %% it.
                    ko; %% TODO: the above.
                {_, [_|_], _} ->
                    %% Some nodes have older versions than us. We need
                    %% to update them. We have this case here so that
                    %% we don't need to worry about nodes being in
                    %% 'shutdown' later on: when everyone involved in
                    %% 'this' config has 'this' config then none of
                    %% them should be reporting shutdown: they should
                    %% all either be transitioning to 'this' config or
                    %% already in 'this' config.
                    update_remote_nodes(OlderThanUs, State);
                {[_|_], [], [{?MODULE, _}]} ->
                    %% Everyone here so far is *new* and has the right
                    %% config, but not everyone is here. We need to
                    %% wait.
                    %% TODO: DON'T WAIT IF WE'RE GOSPEL
                    delayed_request_status(State);
                {_, [], _} ->
                    %% There may be some bad nodes around, but some
                    %% nodes are not new. We need to investigate
                    %% further.
                    case dict:find(ok, TransDict) of
                        {ok, Nodes} ->
                            %% Some nodes are not transitioning at
                            %% all: they are up and running. We should
                            %% be able to sync to them and come up.
                            ko; %% TODO: the above
                        error ->
                            %% Ok, so the other nodes must be
                            %% rejoining. We should just wait for them
                            %% to be ready.
                            %% TODO: DON'T WAIT IF WE'RE GOSPEL
                            delayed_request_status(State)
                    end
            end;
        _ ->
            {config_changed, Youngest}
    end;
event({delayed_request_status, Ref},
      State = #state { state = {delayed_request_status, Ref} }) ->
    request_status(State);
event({request_status, _Ref}, State) ->
    %% ignore it
    {continue, State}.


request_status(State = #state { comms  = Comms,
                                config = #config { nodes = Nodes } }) ->
    NodesNotUs = [ N || {N, _Mode} <- Nodes, N =/= node() ],
    ok = rabbit_clusterer_comms:multi_call(NodesNotUs, request_status, Comms),
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
