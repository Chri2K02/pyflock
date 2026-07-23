"""Node liveness registry, backed by Redis heartbeat keys.

The single source of truth for "is this node alive?" is the existence of its
heartbeat key, which the node refreshes on an interval with a TTL. When the node
stops (crash, kill, network partition) the key expires and the node is
considered dead. Durable node *metadata* lives in Postgres via the repository;
this module only concerns itself with liveness in Redis.
"""

from __future__ import annotations

import redis

from pyflock.core import keys


def mark_alive(r: redis.Redis, node_id: str, ttl: int) -> None:
    """Register the node and set its heartbeat with a fresh TTL."""
    pipe = r.pipeline()
    pipe.sadd(keys.NODES, node_id)
    pipe.set(keys.heartbeat(node_id), "1", ex=ttl)
    pipe.execute()


def refresh(r: redis.Redis, node_id: str, ttl: int) -> None:
    """Renew a node's heartbeat TTL. Called on every heartbeat tick."""
    # SADD is cheap and keeps the node discoverable even if NODES was trimmed.
    pipe = r.pipeline()
    pipe.sadd(keys.NODES, node_id)
    pipe.set(keys.heartbeat(node_id), "1", ex=ttl)
    pipe.execute()


def is_alive(r: redis.Redis, node_id: str) -> bool:
    """True while the node's heartbeat key still exists."""
    return bool(r.exists(keys.heartbeat(node_id)))


def known_ids(r: redis.Redis) -> set[str]:
    """All node ids the cluster has ever registered."""
    return set(r.smembers(keys.NODES))


def dead_ids(r: redis.Redis) -> list[str]:
    """Known node ids whose heartbeat has expired."""
    return [nid for nid in known_ids(r) if not is_alive(r, nid)]


def forget(r: redis.Redis, node_id: str) -> None:
    """Remove a node from the registry entirely (after its work is reclaimed)."""
    pipe = r.pipeline()
    pipe.srem(keys.NODES, node_id)
    pipe.delete(keys.heartbeat(node_id))
    pipe.execute()
