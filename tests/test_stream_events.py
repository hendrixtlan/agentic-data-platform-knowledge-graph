import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from kg_lab.streaming.demo import DATA, generate_demo_events
from kg_lab.streaming.events import MetadataEvent, make_event, partition_key
from kg_lab.streaming.transform import assess_schema_change, transform_event


class FakeRegistry:
    def __init__(self):
        self.versions = []

    def register(self, subject, schema):
        if schema not in self.versions:
            self.versions.append(schema)
        return {"id": self.versions.index(schema) + 1}

    def version(self, subject, version="latest"):
        i = (len(self.versions) - 1) if version == "latest" else (int(version) - 1)
        return {"version": i + 1, "id": i + 1, "schema": json.dumps(self.versions[i])}

    def compatibility(self, subject):
        return "BACKWARD"

    def test_compatibility(self, subject, schema, version="latest"):
        return False


def test_event_validation_and_stable_exact_replay():
    at = datetime(2026, 10, 14, tzinfo=timezone.utc)
    payload = {"subject": "foo", "topic": "foo", "schema": {}}
    a = make_event("schema_version_observed", "registry", payload, "foo", at)
    b = make_event("schema_version_observed", "registry", payload, "foo", at)
    assert a.event_id == b.event_id
    assert a.fingerprint() == b.fingerprint()
    assert MetadataEvent.model_validate_json(a.to_json()) == a
    assert partition_key(a) == "subject:foo"
    changed_time = make_event("schema_version_observed", "registry", payload, "foo",
                              datetime(2026, 10, 15, tzinfo=timezone.utc))
    assert changed_time.event_id != a.event_id


def test_timezone_required():
    with pytest.raises(ValidationError):
        MetadataEvent(event_id="x", event_type="dbt_manifest_snapshot", source="dbt",
                      occurred_at=datetime(2026, 10, 14), payload={})


def test_demo_scenario_events_are_structurally_valid():
    registry = FakeRegistry()
    initial = generate_demo_events("baseline", registry)
    changed = generate_demo_events("changed", registry)
    assert len(initial) == 3
    assert len(changed) == 2
    assert initial[0].payload["version"] == 1
    assert changed[0].payload["version"] == 2
    assert changed[1].payload["registry_compatible"] is False
    assert [x.event_type for x in initial] == [
        "schema_version_observed", "openlineage_run_event", "dbt_manifest_snapshot"]
    for event in initial + changed:
        mutation = transform_event(event)
        assert mutation.entities
        assert mutation.relationships


def test_schema_versions_keep_identity_and_pointer_instruction():
    registry = FakeRegistry()
    event = generate_demo_events("baseline", registry)[0]
    batch = transform_event(event)
    assert batch.current_schema_version == (
        "subject:customer_events-value", "version:customer_events-value:1", 1)
    assert ("KafkaTopic", "topic:customer_events") in [(e.label, e.id) for e in batch.entities]
    assert "USES_SUBJECT" in {r.rel_type for r in batch.relationships}


def test_heuristic_is_not_compatibility_engine():
    old = json.loads((DATA / "customer_v2.avsc.json").read_text())
    proposed = json.loads((DATA / "customer_breaking_proposal.avsc.json").read_text())
    assessment = assess_schema_change(old, proposed)
    assert assessment["removed"] == ["customer_id"]
    assert assessment["modified"] == ["event_type"]
    assert assessment["review_required"] is True
    assert "Heuristic" in assessment["note"]


def test_failed_run_does_not_write_success_edges():
    registry = FakeRegistry()
    e = generate_demo_events("baseline", registry)[1]
    failed = e.model_copy(deep=True)
    failed.payload["eventType"] = "FAIL"
    batch = transform_event(failed)
    assert any(x.label == "LineageRun" for x in batch.entities)
    assert {r.rel_type for r in batch.relationships} == {"EXECUTES"}


def test_real_dbt_manifest_dependencies():
    registry = FakeRegistry()
    event = generate_demo_events("baseline", registry)[2]
    batch = transform_event(event)
    assert "dbt:customer_360" in batch.reconcile_dbt_models
    assert ("READS_FROM", "dataset:bronze.customer") in batch.reconcile_dbt_models["dbt:customer_360"]
    assert ("PRODUCES", "dataset:gold.customer_360") in batch.reconcile_dbt_models["dbt:customer_360"]


def test_invalid_input_rejected_before_graph_write():
    at = datetime(2026, 10, 14, tzinfo=timezone.utc)
    event = make_event("schema_version_observed", "reg", {"subject": "x", "topic": "x", "version": 0,
                         "schema_id": 1, "schema": {"type": "record"}}, "x", at)
    with pytest.raises(ValueError, match="schema version"):
        transform_event(event)
