from datetime import datetime, timezone

import pytest

from kg_lab.streaming.apply import EventConflict, _write_transaction
from kg_lab.streaming.events import make_event
from kg_lab.streaming.transform import GraphMutation


class Result:
    def __init__(self, record=None):
        self.record = record

    def single(self):
        return self.record

    def consume(self):
        pass


class FakeTransaction:
    def __init__(self):
        self.events = {}
        self.queries = []

    def run(self, query, **params):
        self.queries.append((query, params))
        if "MERGE (e:AppliedEvent" in query:
            event = self.events.setdefault(params["event_id"], {
                "status": "PENDING", "fingerprint": params["fingerprint"]})
            return Result(event.copy())
        if "SET e.status = 'APPLIED'" in query:
            self.events[params["event_id"]]["status"] = "APPLIED"
        return Result()


def test_idempotent_replay_skips_projection():
    event = make_event("dbt_manifest_snapshot", "dbt_artifact", {"manifest": {"models": []}},
                       "dbt", datetime(2026, 10, 14, tzinfo=timezone.utc))
    tx = FakeTransaction()
    assert _write_transaction(tx, event, GraphMutation()) == "applied"
    n = len(tx.queries)
    assert _write_transaction(tx, event, GraphMutation()) == "duplicate"
    assert len(tx.queries) == n + 1
    assert tx.events[event.event_id]["status"] == "APPLIED"


def test_reused_id_with_different_payload_fails_loudly():
    event = make_event("dbt_manifest_snapshot", "dbt_artifact", {"manifest": {"models": []}},
                       "dbt", datetime(2026, 10, 14, tzinfo=timezone.utc))
    tx = FakeTransaction()
    _write_transaction(tx, event, GraphMutation())
    poisoned = event.model_copy(deep=True)
    poisoned.payload["manifest"]["models"].append({"name": "unexpected"})
    with pytest.raises(EventConflict, match="Conflicting payload"):
        _write_transaction(tx, poisoned, GraphMutation())


def test_atomic_projection_contains_version_monotonicity_guard():
    from kg_lab.streaming.demo import generate_demo_events
    from test_stream_events import FakeRegistry
    event = generate_demo_events("baseline", FakeRegistry())[0]
    from kg_lab.streaming.transform import transform_event
    tx = FakeTransaction()
    assert _write_transaction(tx, event, transform_event(event)) == "applied"
    statements = "\n".join(query for query, _ in tx.queries)
    assert "coalesce(s.latest_version, 0) < $version" in statements
    assert "CURRENT_VERSION" in statements
    assert "MERGE (a)-[r:HAS_VERSION]->(b)" in statements
