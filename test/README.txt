Testing Plan 3.

# Plan 1 was:
program <- dsl
run program
verify result.

Problem was generation of valid program required complex linear types,
making in unfeasible. I then went to plan 2:

# Plan 2 was:
one process per node. Up to one config process per node.
Iterative expansion of program with minimal coordination and state by
essentially allowing node processes to do whatever they want to their
node.

Problem here is that the node processes can't really validate the
action they attempted to apply to their node as their node is
influenced by everyone else too. Eg you start a node, but you can't
even expect to find later that it's up because some other node process
may have applied a config to their node which turns our node off.

General process was to ask everyone to "observe and pick your next
action", then "apply action". Repeat.

# Plan 3 is:
Sort of a variation on Plan 2, but with much greater coordination.

Driver asks all nodes for their instruction. Node process select based
on current known state of their node. Driver uses all this to predict
the next stable state. Driver allows all node processes to proceed,
which they do. We then continuously poll all nodes until they're
stable - i.e. pending_shutdown, off, or read. At that point, we
compare global state with predicted state.

Now there are large areas which will not be tested by plan 3
(i.e. application of changes during times of flux), but it's the
sanest approach to testing yet, and maybe implementable.
