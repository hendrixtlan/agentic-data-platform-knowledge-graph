"""Reproducible event publisher; generates real Registry versions + synthetic lineage."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from kg_lab.streaming.events import make_event
from kg_lab.streaming.registry import SchemaRegistryClient

DATA = Path(__file__).resolve().parents[3] / "data" / "events"
SUBJECT = "customer_events-value"
TOPIC = "customer_events"


def _json(filename: str) -> dict:
    return json.loads((DATA / filename).read_text(encoding="utf-8"))


def generate_demo_events(phase: str, registry: SchemaRegistryClient) -> list:
    """Schema registration happens against a real Registry endpoint.

    The second phase includes an UNREGISTERED, deliberately problematic change
    proposal; it never pretends an incompatible schema was accepted by Registry.
    """
    if phase not in {"baseline", "changed"}:
        raise ValueError("phase must be baseline or changed")
    v1, v2 = _json("customer_v1.avsc.json"), _json("customer_v2.avsc.json")
    # Registry is the source of truth for accepted version and schema ID.
    if phase == "baseline":
        registry.register(SUBJECT, v1)
    else:
        registry.register(SUBJECT, v1)  # repeatable even when baseline already ran
        registry.register(SUBJECT, v2)
    response = registry.version(SUBJECT, "1" if phase == "baseline" else "latest")
    accepted = json.loads(response["schema"])
    events = [make_event("schema_version_observed", "schema_registry", {
        "topic": TOPIC,
        "subject": SUBJECT,
        "version": response["version"],
        "schema_id": response["id"],
        "compatibility": registry.compatibility(SUBJECT),
        "schema": accepted,
    }, f"{SUBJECT}:{response['version']}")]

    if phase == "baseline":
        lineage = _json("openlineage_customer_complete.json")
        lineage["eventTime"] = datetime.now(timezone.utc).isoformat()
        lineage["run"]["runId"] = str(uuid4())
        events += [
            make_event("openlineage_run_event", "openlineage", lineage, lineage["run"]["runId"]),
            make_event("dbt_manifest_snapshot", "dbt_artifact", {"manifest": _json("dbt_manifest_real_minimal.json")}, "customer_models"),
        ]
    else:
        proposed = _json("customer_breaking_proposal.avsc.json")
        # Real registry compatibility check; informational only, never register.
        compatible = registry.test_compatibility(SUBJECT, proposed)
        proposal = {
            "subject": SUBJECT,
            "proposal_id": "remove-customer-id",
            "current_schema": accepted,
            "proposed_schema": proposed,
            "registry_compatible": compatible,
        }
        events.append(make_event("schema_change_proposed", "schema_registry_review", proposal, "remove-customer-id"))
    return events
