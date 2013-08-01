What to model?

Or, put it another way, if we're generating instructions, how can
we ensure that these instructions are valid? Instructions can be
dependent on the state of the cluster/clusters.

Instructions:
change shutdown timeout (which config?)
change gospel (which config?)
add node to cluster (which config?)
remove node from cluster (which config?)

start node
stop node
reset node
delete node

apply config (which config? which node?)


If we permit "apply config" to apply to stopped nodes then we
keep "start node" much simpler, though we'd need to track...

New idea. Iterative expansion based on observations of the real
world. One erlang process per node.

Now either that process can also evolve config, or separate processes
for config evolution. Up to max of one config process per node based
on fact you can't apply multiple configs to the same node at once.
