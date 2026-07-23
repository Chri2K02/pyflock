"""Node liveness registry."""

from __future__ import annotations

from pyflock.core import keys, registry


def test_mark_alive_registers_and_is_alive(redis_client):
    registry.mark_alive(redis_client, "node-1", ttl=10)

    assert registry.is_alive(redis_client, "node-1") is True
    assert "node-1" in registry.known_ids(redis_client)


def test_dead_ids_reports_nodes_without_heartbeat(redis_client):
    registry.mark_alive(redis_client, "alive-node", ttl=10)
    registry.mark_alive(redis_client, "dead-node", ttl=10)
    # Simulate the heartbeat expiring for one node.
    redis_client.delete(keys.heartbeat("dead-node"))

    assert registry.is_alive(redis_client, "dead-node") is False
    assert registry.dead_ids(redis_client) == ["dead-node"]


def test_forget_removes_node_entirely(redis_client):
    registry.mark_alive(redis_client, "node-1", ttl=10)
    registry.forget(redis_client, "node-1")

    assert "node-1" not in registry.known_ids(redis_client)
    assert registry.is_alive(redis_client, "node-1") is False


def test_ttl_is_set_on_heartbeat(redis_client):
    registry.mark_alive(redis_client, "node-1", ttl=10)
    ttl = redis_client.ttl(keys.heartbeat("node-1"))
    assert 0 < ttl <= 10
