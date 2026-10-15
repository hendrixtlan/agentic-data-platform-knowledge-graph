from kg_lab.streaming.watch import discover_versions
from test_stream_events import FakeRegistry
from kg_lab.streaming.demo import DATA
import json


def test_watcher_backfills_intermediate_versions():
    registry = FakeRegistry()
    registry.register("customer_events-value", json.loads((DATA / "customer_v1.avsc.json").read_text()))
    registry.register("customer_events-value", json.loads((DATA / "customer_v2.avsc.json").read_text()))
    backlog = discover_versions(registry, "customer_events-value", "customer_events", 0)
    assert [event.payload["version"] for event in backlog] == [1, 2]
    assert {event.event_type for event in backlog} == {"schema_version_observed"}
    assert discover_versions(registry, "customer_events-value", "customer_events", 2) == []
    assert backlog[0].payload["schema_id"] == 1
