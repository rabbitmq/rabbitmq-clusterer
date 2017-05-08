# RabbitMQ Clusterer

This plugin provides an alternative means for creating and maintaining
RabbitMQ clusters. It is highly opinionated and was created with specific
opinionated infrastructure provisioning tooling in mind.

In case you need a plugin that provides cluster member discovery, take a look at [rabbitmq-autocluster](https://github.com/aweber/rabbitmq-autocluster)
first. That plugin is not a strict alternative to this one but targets
a wider range of provisioning scenarios.

## Project status

The plugin was created to handle arbitrary order on nodes restart.
Since RabbitMQ version 3.6.7 this problem is addressed in the core.

This plugin is considered deprecated, and it's recommended to switch
to RabbitMQ's built-in [cluster formation feature](http://www.rabbitmq.com/configure.html) in order to avoid
known issues that this plugin's opinionated behavior entails (such as
[#7](https://github.com/rabbitmq/rabbitmq-clusterer/issues/7)).

## Overview

Traditional RabbitMQ clustering is not very friendly to infrastructure
automation tools such as Chef, Puppet or BOSH. The
existing tooling (`rabbitmqctl join_cluster` and friends) is
imperative, requires more oversight and does not handle potentially
random node boot order very well. The Clusterer has been specifically
designed with automated deployment tools in mind.

Unlike the existing tooling, the Clusterer is declarative and goal
directed: you tell it the overall shape of the cluster you want to
construct and the clusterer tries to achieve that. With the Clusterer,
cluster configuration can be provided in a single location (a configuration
file).

With `rabbitmq-clusterer`, nodes in a cluster can be restarted in any order,
which can be the case with automation tools performing upgrades/reconfiguration,
or due to node failure timing.


## Project Maturity

This plugin is considered production ready. It has been used in Pivotal
projects since summer 2013 and used heavily in a high profile product.


## Compatibility With Traditional RabbitMQ Clustering

The Clusterer is not generally compatible with the existing clustering
tool-set. Do not use any of the `rabbitmqctl` commands relating to
changing clusters: `join_cluster`, `change_cluster_node_type`, and `update_cluster_nodes` must not be used.
If you do use these, this plugin likely won't be able to perform its jobs.

`rabbitmqctl cluster_status` may be used to inspect a cluster
state, but the Clusterer sends to the standard Rabbit log files
details about any clusters it joins or leaves. See the *Inspecting the
Clusterer Status* section further down.

`rabbitmqctl stop_app`, `rabbitmqctl forget_cluster_node`, and `rabbitmqctl start_app`
can be used to force a node out of a cluster before cluster config can be changed. While
this is not generally recommended, there can be valid reasons for doing so, e.g. node
running out of disk space and/or needing replacement for other reasons.

`cluster_nodes` in the RabbitMQ config file is incompatible with this plugin
and must not be used.


## Installation

Binary builds of this plugin are available from

 * [Bintray](https://bintray.com/rabbitmq/community-plugins/rabbitmq_clusterer) (like with other [RabbitMQ Community Plugins](http://rabbitmq.com/community-plugins.html))
 * [GitHub releases page](https://github.com/rabbitmq/rabbitmq-clusterer/releases)

As with all other plugins, you must put the plugin archive (`.ez`) in
the [RabbitMQ plugins directory](http://www.rabbitmq.com/relocate.html)
and enable it with `rabbitmq-plugins enable rabbitmq_clusterer --offline`.

## For Recent RabbitMQ Versions (3.5.4 and Later)

Compiled plugin file needs to be placed into .

To use the plugin, it is necessary to override `RABBITMQ_BOOT_MODULE` to `rabbit_clusterer`. This
is done similarly to [other RabbitMQ environment variables](http://rabbitmq.com/configure.html).

Because this plugin coordinates RabbitMQ node start, it needs to be manually added to the Erlang VM
code path:

```
export RABBITMQ_BOOT_MODULE=rabbit_clusterer
export RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-pa /path/to/rabbitmq/plugins/rabbitmq_clusterer.ez/rabbitmq_clusterer-{clusterer-version}/ebin"
```

where `{clusterer-version}` is the build of the plugin (see [GitHub releases page](https://github.com/rabbitmq/rabbitmq-clusterer/releases) and [Bintray](https://bintray.com/rabbitmq/community-plugins/rabbitmq_clusterer)):

```
export RABBITMQ_BOOT_MODULE=rabbit_clusterer
export RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-pa /path/to/rabbitmq/plugins/rabbitmq_clusterer-1.0.3.ez/rabbitmq_clusterer-1.0.3/ebin"
```

Since `.ez` files are `.zip` archives, they can be easily inspected when you are not sure about
the exact name of the directory you the file you've downloaded.

>>>>>>> stable

## For RabbitMQ 3.4.x

### rabbitmq-server Patch

With RabbitMQ versions earlier than `3.5.4`, it is necessary to apply a `rabbitmq-server`
patch and re-compile the broker.

The patch is provided in
[rabbitmq-clusterer/rabbitmq-server.patch](https://github.com/rabbitmq/rabbitmq-clusterer/blob/master/rabbitmq-server.patch).
Change into the server scripts
directory and apply it with:

    patch -p1 < rabbitmq-clusterer/rabbitmq-server.patch

The patch assumes the plugin archive is at `${RABBITMQ_PLUGINS_DIR}/rabbitmq_clusterer.ez`.


## Usage in Environments with Dynamic Hostnames (e.g. Kubernetes)

Since this plugin assumes that all cluster members are known ahead of time
and listed in the config, environments with dynamically generated hostnames
must be configured to use known (or completely predictable) hostnames.

For Kubernetes specifically, there's an [example repository](https://github.com/MattFriedman/kubernetes-rabbitmq-clusterer) contributed by Matt Friendman.

There's also [another example that uses Kubernetes 1.5.x](https://github.com/nanit/kubernetes-rabbitmq-cluster).


## Cluster Config Specification

The Clusterer will communicate a new valid config to both all the
nodes of its current config, and in addition to all the nodes in the
new config. Even if the cluster is able to be formed in the absence of
some nodes indicated in the config, the nodes of the cluster will
continue to attempt to make contact with any missing nodes and will
pass the config to them if and when they eventually appear.

All of which means that you generally only need to supply new configs
to a single node of any cluster. There is no harm in doing more than
this. The Clusterer stores on disk the currently applied config (it
stores this next to the mnesia directory Rabbit uses for all its
persistent data) and so if a node goes down, it will have a record of
the config in operation when it was last up. When it comes back up, it
will attempt to rejoin that cluster, regardless of whether this node
was ever explicitly given this config.

There are a couple of ways to specify a cluster config: via an external
file or inline.

### Using External Config File

In a `rabbitmq_clusterer` section in `rabbitmq.config` file you
can add a `config` entry that is a path to configuration file.

Below are some examples.

When using `rabbitmq.conf` (currently only available in RabbitMQ master):

    clusterer.config = /path/to/my/cluster.config

When using the classic configuration format (`rabbitmq.config`, prior to 3.7.0) or `advanced.config`:

      [{rabbitmq_clusterer,
          [{config, "/path/to/my/cluster.config"}]
       }].

Like with `rabbitmq.config` or any other Erlang terms file,
the dot at the end is mandatory.

### Using Inline Configuration in rabbitmq.config

It is possible to provide cluster configuration in `rabbitmq.config`.

In `rabbitmq.conf`:

    clusterer.version = 43
    clusterer.nodes.disc.1 = rabbit@hostA
    clusterer.nodes.disc.2 = rabbit@hostD
    clusterer.nodes.ram.1  = rabbit@hostB
    clusterer.gospel.node = rabbit@hostD

Or, using the classic config format (`rabbitmq.config`, prior to 3.7.0) or `advanced.config`:

      [{rabbitmq_clusterer,
          [{config,
              [{version, 43},
               {nodes, [{rabbit@hostA, disc}, {rabbit@hostB, ram}, {rabbit@hostD, disc}]},
               {gospel, {node, rabbit@hostD}}]
           }]
       }].

This approach makes configuration management with tools such as Chef somewhat
less convenient, so external configuration file is the recommended option.


### Using rabbitmqctl eval

`rabbitmqctl eval 'rabbit_clusterer:apply_config().'`

**This will only have any effect if there is an entry in the
`rabbitmq.config` file for the Clusterer as above, and a path is
specified as the value rather than a config directly.**

If that is the case, then this will cause the node to reload the
file containing cluster config and apply it. Note that you cannot
change the path itself in the `rabbitmq.config` file dynamically:
neither Rabbit nor the Clusterer will pick up any changes to that
file without restarting the whole Erlang node.

`rabbitmqctl eval 'rabbit_clusterer:apply_config("/path/to/my/other/cluster.config").'`

This will cause the Clusterer to attempt to load the indicated file
as a cluster config and apply it. Using this method rather than the
above allows the path to change dynamically and does not depend on
any entries in the `rabbitmq.config` file. The path provided here is
not retained in any way: providing the path here does not influence
future calls to `rabbit_clusterer:apply_config().` - using
`rabbit_clusterer:apply_config().` *always* attempts to inspect the
path as found in `rabbitmq.config` when the node was started.

Note if you really want to, rather than suppling a path to a file,
you can supply the cluster config as a proplist directly, just as
you can in the `rabbitmq.config` file itself.



## Cluster Configuration

A cluster config is an Erlang proplist consisting of just four
tuples. The config can be supplied to Rabbit in a variety of ways and
it is in general only necessary to supply a config to a single node of
a cluster: the Clusterer will take care of distributing the config to
all the other nodes as necessary.

    [{version, 43},
     {nodes, [{rabbit@hostA, disc}, {rabbit@hostB, ram}, {rabbit@hostD, disc}]},
     {gospel, {node, rabbit@hostD}}].

The above gives an example cluster config. This specifies that the
cluster is formed out of the nodes `rabbit@hostA`, `rabbit@hostB` and
`rabbit@hostD` and that `rabbit@hostA` and `rabbit@hostD` are *disc*
nodes and `rabbit@hostB` is a *ram* node. The `nodes` tuple is really
the only tuple that describes the shape of the cluster. The other
tuples describe how to achieve the cluster, and are thus mainly
irrelevant once the cluster has been achieved.

In general, the Clusterer will wait indefinitely for the conditions to
be correct to form any given cluster. This is in contrast to the
existing tools which will either timeout or in some cases take
(arguably) unsafe actions. For example, the existing tools will allow
a fresh node to fully start when it is supplied with a cluster
configuration which involves other nodes which are not currently
contactable. This is unsafe because those other nodes might not be
fresh nodes: the intention would be for the fresh node to sync with
those other nodes and preserve the data those nodes hold. When those
other nodes eventually return, manual intervention is then required to
throw away some data and preserve others. The Clusterer, by contrast,
would wait until it could either verify that all the nodes to be part
of the cluster are fresh (so there is no data to preserve at all), or
failing that would wait until one of the non-fresh nodes was fully up
and running, at which point it could sync with that node.

* version: non negative integer

    All configs are versioned and this is used to decide which of any
    two configs is the youngest. A config which has a smaller version
    number is older. Configs will be ignored unless they are younger
    than the current config. Note that in lieu of any config being
    provided by the user, the default config is used which has a
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

    * `{node, nodename}` The nodename must appear in the `nodes`
      tuple. The data held by the existing cluster of which *nodename*
      is a member will survive. Nodes that are listed in the `nodes`
      tuple but which are not currently members of the same cluster as
      *nodename* will be reset. The phrasing here is very deliberate:
      it is not necessary for *nodename* to actually be up and running
      for this to work. If you have an existing cluster of nodes *A*
      and *B* and you want to add in node *C* you can set the `gospel`
      to be `{node, A}`, add *C* to the `nodes` tuple, bump the
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
      config will propagate to *B* and *C*. If *A* is named as the
      gospel in the new younger config, then that refers to the data
      held by the new younger *A*, and so *B* and *C* will reset as
      necessary.



### Mistakes in config files

Config files can contain mistakes. If you apply a config file using
`rabbitmqctl eval` then you'll get feedback directly. If you specify
the config file via `rabbitmq.config` then and mistakes will be logged
to Rabbit's log files.

In general, the Clusterer tries reasonably hard to give informative
messages about what it doesn't like, but that can only occur if the
config is syntactically valid in the first place. If you forget to
bump the version number it will complain, and generally whenever the
Clusterer comes across configs with equal version numbers but
semantically different contents it takes highly evasive action: in
some situations, it may decide to shut down the whole Erlang node
immediately. It is your responsibility to manage the version numbers:
the Clusterer expects to be able to order configs by version numbers,
and thus determine the youngest config. You need to ensure it can do
this. If you're building cluster configs automatically, one sensible
approach would be to set the version to the number of seconds since
epoch, for example.


## Inspecting the Clusterer Status

`rabbitmqctl cluster_status` presents basic information about
clusters, but does not interact with the Clusterer. `rabbitmqctl eval
'rabbit_clusterer:status().'`, on the other hand, does, and shows
which config is in operation by the node and what the Clusterer is
trying to do. If the cluster has been established then the command
will also display which nodes are known to be currently up and
running.


## Building From Source

The Clusterer reuses parts of the RabbitMQ [umbrella repository](https://github.com/rabbitmq/rabbitmq-public-umbrella). Before
building the plugin, make sure it is cloned as `rabbitmq_clusterer` under it,
much [like other plugins](https://www.rabbitmq.com/plugin-development.html).

To build the plugin run `make`. The `VERSION` environment variable is used to specify plugin version, e.g.:

    VERSION=3.6.6 make

To package the plugin run `make dist`. In some cases, `make clean dist` is the
safest option.

### Linking in Development Environment

If you're running a development environment and want to link through
from the `rabbit/plugins` directory, link to
`rabbitmq_clusterer/plugins/rabbitmq_clusterer-$VERSION.ez`. Do not just
link to the `rabbitmq_clusterer` directory.


## License and Copyright

(c) 2013-2017 Pivotal Software Inc.
