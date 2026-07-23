"""Central definition of every Redis key pyflock uses.

Keeping key construction in one place avoids typos and makes the data model
easy to reason about (and easy to inspect with ``redis-cli KEYS pyflock:*``).
"""

from __future__ import annotations

PREFIX = "pyflock"

# Main FIFO queue of job ids waiting to be pulled by a worker.
QUEUE = f"{PREFIX}:queue"

# Sorted set of jobs scheduled for a future retry, scored by unix timestamp.
DELAYED = f"{PREFIX}:delayed"

# List of job ids that exhausted their retry budget.
DEAD_LETTER = f"{PREFIX}:dead_letter"

# Set of all node ids the cluster has seen.
NODES = f"{PREFIX}:nodes"


def heartbeat(node_id: str) -> str:
    """Key holding a node's heartbeat; its TTL expiring means the node is dead."""
    return f"{PREFIX}:node:{node_id}:heartbeat"


def processing(node_id: str) -> str:
    """Per-node list of jobs currently checked out by that node.

    A job is moved here atomically when pulled (via ``BRPOPLPUSH``) and removed
    on completion. If the node dies, the reaper drains this list back onto the
    main queue — this is what makes orphaned jobs recoverable.
    """
    return f"{PREFIX}:processing:{node_id}"
