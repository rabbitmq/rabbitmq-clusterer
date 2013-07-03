-module(rabbit_clusterer).

-rabbit_boot_step(
   {rabbit_clusterer,
    [{description, "Declarative Clustering"},
     {mfa, {?MODULE, start_and_await_cluster, []}},
     {enables, database}]}).

-behaviour(gen_server).

-export([start_and_await_cluster/0, await_cluster/0, become/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, { status,
                 awaiting_active,
                 node_id,
                 target_config,
                 current_config
               }).

-include("rabbit_clusterer.hrl").

start_and_await_cluster() ->
    ok = rabbit_sup:start_child(?MODULE),
    %% We'll tidy this up later, but for the time being, we're just
    %% happy to explode at this point if the world is not fixable.
    ok = await_cluster().

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

await_cluster() ->
    gen_server:call(?SERVER, await_cluster, infinity).

%%----------------------------------------------------------------------------

init([]) ->
    ok = rabbit_mnesia:ensure_mnesia_dir(),
    State = #state { status          = inactive,
                     awaiting_active = [],
                     node_id         = undefined,
                     target_config   = undefined,
                     current_config  = undefined },
    State1 = case ensure_node_id() of
                 {ok, NodeId} -> State #state { node_id = NodeId };
                 Err1         -> set_status(Err1, State)
             end,
    {TargetConfig, CurrentConfig} = choose_config(),
    State2 = State1 #state { target_config = TargetConfig,
                             current_config = CurrentConfig },
    State3 = case load_last_seen_cluster_state() of
                 {ok, {AllNodes, DiscNodes, RunningNodes}} ->
                     State2;
                 Err2 ->
                     set_status(Err2, State2)
             end,
    {ok, State3}.

handle_call(await_cluster, From, State = #state { status = inactive,
                                                  awaiting_active = AA }) ->
    {noreply, State #state { awaiting_active = [From | AA] }};
handle_call(await_cluster, _From, State = #state { status = Status }) ->
    {reply, Status, State};
handle_call(Msg, From, State) ->
    {stop, {unhandled_call, Msg, From}, State}.

handle_cast(Msg, State) ->
    {stop, {unhandled_cast, Msg}, State}.

handle_info(Msg, State) ->
    {stop, {unhandled_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

node_id_file_path() ->
    filename:join(rabbit_mnesia:dir(), "node_id").

internal_config_path() ->
    filename:join(rabbit_mnesia:dir(), "cluster.config").

external_config_path() ->
    application:get_env(rabbit, cluster_config).

set_status_ok(State) ->
    set_status(ok, State).

set_status(Term, State = #state { awaiting_active = AA,
                                  status = inactive }) ->
    [gen_server:reply(From, Term) || From <- AA],
    State #state { awaiting_active = [], status = Term }.

ensure_node_id() ->
    case rabbit_file:read_term_file(node_id_file_path()) of
        {ok, [NodeId]}    -> {ok, NodeId};
        {error, enoent}   -> create_node_id();
        {error, _E} = Err -> Err
    end.

create_node_id() ->
    %% We can't use rabbit_guid here because it hasn't been started at
    %% this stage. In reality, this isn't a massive problem: the
    %% implication that we need to create a node_id is that the guid
    %% serial will be 0 anyway.
    NodeID = erlang:md5(term_to_binary({node(), make_ref()})),
    case rabbit_file:write_term_file(node_id_file_path(), [NodeID]) of
        ok                -> {ok, NodeID};
        {error, _E} = Err -> Err
    end.

load_last_seen_cluster_state() ->
    try {ok, rabbit_node_monitor:read_cluster_status()}
    catch {error, Err} -> {error, Err}
    end.

choose_config() ->
    ExternalProplist =
        case external_config_path() of
            undefined ->
                undefined;
            {ok, ExternalPath} ->
                case rabbit_file:read_term_file(ExternalPath) of
                    {error, enoent}           -> undefined;
                    {ok, [ExternalProplist1]} -> ExternalProplist1
                end
        end,
    InternalProplist = case rabbit_file:read_term_file(internal_config_path()) of
                           {error, enoent}          -> undefined;
                           {ok, [InternalProplist1]} -> InternalProplist1
                       end,
    case {ExternalProplist, InternalProplist} of
        {undefined, undefined} ->
            {rabbit_clusterer_config:default_config(), undefined};
        {undefined, Proplist} ->
            Config = rabbit_clusterer_config:proplist_config_to_record(Proplist),
            {Config, Config};
        {Proplist, undefined} ->
            {rabbit_clusterer_config:proplist_config_to_record(Proplist), undefined};
        _ ->
            [ExternalConfig = #config { version = ExternalV,
                                        minor_version = ExternalMV },
             InternalConfig = #config { version = InternalV,
                                        minor_version = InternalMV }] =
                [rabbit_clusterer_config:proplist_config_to_record(Proplist)
                 || Proplist <- [InternalProplist, ExternalProplist]],
            %% We deliberately require > and not >=. I.e. if a user
            %% provides a config, it must have a strictly greater
            %% version than our current config in order to be valid.
            if
                {ExternalV, ExternalMV} > {InternalV, InternalMV} ->
                    {ExternalConfig, InternalConfig};
                true ->
                    {InternalConfig, InternalConfig}
            end
    end.

%% Action plan.
%%
%% It appears we need to be a state machine.
%%
%% Easy case: old is undefined, new is the default.
%%
%% Next easy case: old is not undefined, new is the default:
%%
%%   Mnesia's limitations actually make this easy for us: we cannot
%%   convince mnesia to disconnect another node and remain
%%   disconnected - it's impossible.  We shouldn't assume at this
%%   point that we've only just come up - this could be a "ctl
%%   reload_cluster_config".

%%   So whenever we see a node-up from someone else, we contact them,
%%   and if our new config is "better" then we just tell them to
%%   switch off. There's an interesting question as to what happens if
%%   their config is the same version as our config.  So we could
%%   force_load everything to make sure we can then
%%   wait_for_tables. We'll then have to just watch to see if anyone
%%   else appears...  ...which turns out to be harder than you'd think
%%   - the node_monitor notify_up is the very last boot step to be
%%   run.
