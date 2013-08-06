# RabbitMQ Clusterer

This plugin provides an alternative means for creating and maintaining
clusters of Rabbits.

Unlike the existing tooling, the Clusterer is declarative and goal
directed: you tell it the overall shape of the cluster you want to
construct and the clusterer tries to achieve that. By contrast, the
existing tooling (`rabbitmqctl join_cluster` and friends) is not goal
directed, requires more oversight, and for these reasons is unsuited
to automated deployment tools. The Clusterer has been specifically
designed with automated deployment tools in mind.

The Clusterer is not compatible with the existing clustering
toolset. Do not use any of the `rabbitmqctl` commands relating to
changing clusters: `join_cluster`, `change_cluster_node_type`,
`forget_cluster_node` and `update_cluster_nodes` must not be used. If
you do use these, behaviour is undefined, and most likely
bad. `rabbitmqctl cluster_status` may be used to inspect a cluster
state, but the Clusterer sends to the standard Rabbit log files
details about any clusters it joins or leaves.

Furthermore, do not specify `cluster_nodes` in the Rabbit config file:
it will be ignored.


## Installation

As with all other plugins, you must put the `.ez` in the right
directory and enable it. If you're running a development environment
and want to link through from the `rabbitmq-server/plugins` directory,
link to `rabbitmq-clusterer/dist/rabbitmq_clusterer-0.0.0.ez`. Do not
just link to the `rabbitmq-clusterer` directory.

Because the Clusterer has to manage Rabbit itself, we have to make a
change to the `rabbitmq-server/scripts/rabbitmq-server` script so that
when Erlang is started, it starts the Clusterer rather than starting
Rabbit. A patch is provided in
`rabbitmq-clusterer/rabbitmq-server.patch`. In a development
environment, apply with:

    rabbitmq-server> patch -p1 < ../rabbitmq-clusterer/rabbitmq-server.patch


## Cluster Configurations

A cluster config is an Erlang proplist consisting of just four
tuples. The config can be supplied to Rabbit in a variety of ways and
it is in general only necessary to supply a config to a single node of
a cluster: the Clusterer will take care of distributing the config to
all the other nodes as necessary.

    [{version, 43},
     {nodes, [{rabbit@hostA, {rabbit@hostB, ram}, {rabbit@hostD, disc}}]},
     {gospel, {node, rabbit@hostD}},
     {shutdown_timeout, 30}]

The above gives an example cluster config. This specifies that the
cluster is formed out of the nodes `rabbit@hostA`, `rabbit@hostB` and
`rabbit@hostD` and that `rabbit@hostA` and `rabbit@hostD` are *disc*
nodes and `rabbit@hostB` is a *ram* node. The `nodes` tuple is really
the only tuple that describes the shape of the cluster. The other
tuples describe how to achieve the cluster, and are thus mainly
irrelevant once the cluster has been achieved.

In general, the Clusterer will wait indefinitely for the conditions to
be correct to form any given cluster. Again, this is in contrast to
the existing tools which will either timeout or in some cases take
unsafe actions.

* version: non negative integer
    
    All configs are versioned and this is used to decide which of any
    any two configs is the youngest. A config which has a smaller
    version number is older. Configs will be ignored unless they are
    younger than the current config. Note that in lieu of any config
    being provided by the user, the default config is used which has a
    version of 0. Thus user supplied configs should use a version of 1
    or greater.

* nodes: list
    
    List the names of the nodes that are to be in the cluster. If you
    list node names directly then they are considered to be disc
    nodes. If you specify nodes by using a tuple, you can specify a
    disc node using either `disc` or `disk`. If you want to specify
    ram nodes, you must use a tuple, with `ram` as the second
    element. Order of nodes does not matter. The following are all
    equivalent.
    
        {nodes, [rabbit@hostA, rabbit@hostD, {rabbit@hostB, ram}]}
        {nodes, [rabbit@hostD, rabbit@hostA, {rabbit@hostB, ram}]}
        {nodes, [{rabbit@hostB, ram}, rabbit@hostD, rabbit@hostA]}
        {nodes, [rabbit@hostA, {rabbit@hostD, disk}, {rabbit@hostB, ram}]}
        {nodes, [{rabbit@hostA, disc}, {rabbit@hostD, disk}, {rabbit@hostB, ram}]}

* gospel: `reset` or `{node, `*nodename*`}`
    
    When multiple nodes are to become a cluster (or indeed multiple
    clusters are to merge: you can think of an unclustered node as a
    cluster of a single node) some data must be lost and some data can
    be preserved: given two unclustered nodes *A* and *B* that are to
    become a cluster, either *A*'s data can survive or *B*`s data can
    survive, or neither, but not both. The `gospel` tuple allows you
    to specify which data should survive:
    
    * `reset` will reset all nodes in the cluster. This will apply
      *every time the cluster config is changed and applied* (i.e. if
      you change some other setting in the config, bump the version
      number, leave the gospel as `reset` and apply the config to any
      node in your cluster, you will find the entire cluster
      resets). This is deliberate: it allows you to very easily and
      quickly reset an entire cluster, but in general you'll only
      occasionally want to set `gospel` to `reset`.
    
    * `{node, `*nodename*`}` The nodename must appear in the `nodes`
      tuple. The data held by the existing cluster of which *nodename*
      is a member will survive. Nodes that are listed in the `nodes`
      tuple but which are not currently members of the same cluster as
      *nodename* will be reset. The phrasing here is very deliberate:
      it is not necessary for *nodename* to actually be up and running
      for this to work. If you have an existing cluster of nodes *A*
      and *B* and you want to add in node *C* you can set the `gospel`
      to be `{node, `*A*`}`, add *C* to the `nodes` tuple, bump the
      version and apply the config to *C* and provided *at least one*
      of *A* or *B* is up and running, *C* will successfully
      cluster. I.e. if only *B* is up, *B* still knows that it is
      clustered with *A*, it just happens to be the case that *A* is
      currently unavailable. Thus *C* can cluster with *B* and both
      will happily work, awaiting the return of *A*.
      
      In this particular case, the subsequent behaviour when *A*
      returns is important. If *A* has been reset and is now running
      an older config then it is *A* that is reset again to join back
      in with *B* and *C*. I.e. the `gospel` setting is really
      identifying that the data that *A* holds at a particular moment
      in time is the data to be preserved. When *A* comes back, having
      been reset, *A* realises that the `gospel` is indicating an
      older version of *A*, which is preserved by the surviving
      cluster nodes of *B* and *C*, not the newer reset data held by
      *A*. The upshot of this is that in your cluster, if a node
      fails, goes down and has to be reset, then to join it back into
      the cluster you don't need to alter anything in the cluster
      config (and indeed shouldn't): even if the failed node was named
      as the `gospel`, you shouldn't make any changes to the config.
      
      By contrast, if *A* comes back and has been reset but is now
      running a younger config than *B* and *C*, then that younger
      config will propogate to *B* and *C*. If *A* is named as the
      gospel in the new younger config, then that refers to the data
      held by the new younger *A*, and so *B* and *C* will reset as
      necessary.

* shutdown_timeout: `infinity` or non negative integer
    
    If a younger config is applied to a node and that node is not
    listed in the `nodes` tuple, then the node should turn off. The
    Clusterer is more than happy to do this. However, for various
    reasons, you might like the Erlang node (and the Clusterer) to
    stay up for some time, even once Rabbit itself has been
    stopped. This would allow you to move a node from one cluster to
    another without ever having to directly interact with that node,
    for example. Another reason would be your automated deployment
    tool might repeatedly check that the Erlang node to run Rabbit is
    alive. If the Clusterer were to actually stop the Erlang node
    entirely then the deployment tool might repeatedly try to start it
    up again only for it to discover it should be off and shut it down
    again, thus a pointless cycle develops.
    
    The `shutdown_timeout` tuple specifies the amount of time, in
    seconds, between the Rabbit application being stopped on the node
    and the Erlang node itself being terminated. Alternatively
    `infinity` can be specified in which case the Erlang node itself
    (and the Clusterer application) is never terminated.
    
    In general, the setting here gives the size of the window during
    which you can grab a node which has left one cluster and join it
    into another. For example, consider you have a cluster of *A*, *B*
    and *C* and you want to remove *C* and join it with *D*. If you
    just apply the *C*+*D* config to *C* then that config will also be
    passed to *A* and *B* who will spot they're not part of this
    config, and so they will shut themselves down - not what you want!
    Instead, apply just an *A*+*B* config to any of *A*, *B* or
    *C*. This will eject *C* from the cluster and for the amount of
    time specified by `shutdown_timeout`, will sit idle, awaiting
    further instructions. You can then apply the *C*+*D* config to
    either *C* or *D*. The *C*+*D* cluster will then form, and that
    config will not be passed to *A*+*B*, so that cluster will survive
    too.
    
    Of course, even if *C* turns all the way off, there's nothing to
    stop you restarting *C* explicitly with the *C*+*D* config: the
    same outcome will be achieved. The `shutdown_timeout` tuple is
    just a convenience to provide a simpler path through this type of
    scenario.
