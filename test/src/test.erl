-module(test).

-export([test/0]).

-include("rabbit_clusterer.hrl").

-record(edge, {config, pre, post}).

test() ->
    io:format("~p~n", [node()]),
    ok.

transitions() ->
    [add_node,
     remove_node,
     merge_node,
     unmerge_node,
     restart_node,
     change_timeout,
     change_gospel,
     reset_cluster].

new_edge() ->
    #edge { config = #config { version          = 0,
                               gospel           = reset,
                               nodes            = [],
                               shutdown_timeout = infinity },
            pre    = [],
            post   = [] }.

add_node(Edge = #edge { config = Config = #config { nodes = Nodes },
                        post = Post }) ->
    %% this is a brand new virgin node coming up and joining into our
    %% cluster.
    Node = new_node(),
    Edge #edge { config = Config #config { nodes = [Node | Nodes] },
                 post   = [verify_cluster_includes(Node) | Post] }.

merge_node(Edge = #edge { config = Config = #config { nodes = Nodes },
                          pre = Pre, post = Post }) ->
    %% We start a brand new node without the config, we then later
    %% verify that it has been joined into our config
    Node = new_node(),
    Edge #edge { pre   = [start_node(Node, undefined) | Pre],
                 post  = [verify_cluster_includes(Node) | Post],
                 config = Config #config { nodes = [Node | Nodes] } }.

