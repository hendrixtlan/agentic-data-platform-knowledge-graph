"""Requires a running Neo4j and RUN_INTEGRATION=1.

CI publishes real Redpanda messages, consumes them, then runs this test.
"""
import os
import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1")
def test_projected_real_kafka_events_and_change_impact():
    from kg_lab.streaming.service import get_impact
    result = get_impact("customer_events-value")
    assert result["found"] is True
    assert result["current_version"] == 2
    assert "customer_events" in result["topics"]
    assert "CustomerStreaming" in result["pipelines"]
    assert "bronze.customer" in result["landing_datasets"]
    assert "customer_360" in result["models"]
    assert "gold.customer_360" in result["output_datasets"]
    assert "Customer Executive Dashboard" in result["dashboards"]
    assert result["proposals"][0]["registry_compatible"] is False
    assert result["proposals"][0]["status"] == "REVIEW_REQUIRED"


@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1")
def test_reprocessing_same_event_is_idempotent():
    from neo4j import GraphDatabase
    from kg_lab.config import settings
    from kg_lab.streaming.apply import apply_event
    with GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)) as driver:
        from kg_lab.streaming.events import MetadataEvent
        # Same envelope across retries: first application creates the version, next is duplicate.
        from datetime import datetime, timezone
        from kg_lab.streaming.events import make_event
        schema = {"type": "record", "name": "AuditExample", "fields": [{"name": "id", "type": "string"}]}
        event = make_event("schema_version_observed", "test", {
            "topic": "test_topic", "subject": "test_topic-value", "version": 1,
            "schema_id": 100000, "schema": schema, "compatibility": "BACKWARD",
        }, "test_topic:1", datetime(2026, 10, 14, tzinfo=timezone.utc))
        first = apply_event(driver, settings.neo4j_database, event)
        second = apply_event(driver, settings.neo4j_database, MetadataEvent.model_validate_json(event.to_json()))
        assert first in {"applied", "duplicate"}
        assert second == "duplicate"
