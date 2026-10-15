"""Pure transformation from incoming metadata events to a canonical graph mutation."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from kg_lab.models import Entity, Relationship
from kg_lab.streaming.events import MetadataEvent


@dataclass
class GraphMutation:
    entities: list[Entity] = field(default_factory=list)
    relationships: list[Relationship] = field(default_factory=list)
    # Only subject:CURRENT_VERSION is mutable in this lab. Versions are append-only.
    current_schema_version: tuple[str, str, int] | None = None
    # dbt-owned model relations may be reconciled against the newest manifest.
    reconcile_dbt_models: dict[str, list[tuple[str, str]]] = field(default_factory=dict)


def _provenance(event: MetadataEvent) -> dict[str, Any]:
    return {"source": event.source, "event_id": event.event_id, "confidence": 1.0}


def _name(raw: Any, field_name: str) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"Missing or invalid {field_name}")
    return raw.strip()


def _fields(schema: dict) -> dict[str, str]:
    return {str(x["name"]): json.dumps(x["type"], sort_keys=True) for x in schema.get("fields", [])}


def assess_schema_change(current: dict, proposed: dict) -> dict[str, Any]:
    """Field-level heuristic only; NOT an Avro compatibility certification."""
    before, after = _fields(current), _fields(proposed)
    removed = sorted(set(before) - set(after))
    added = sorted(set(after) - set(before))
    modified = sorted(k for k in before.keys() & after.keys() if before[k] != after[k])
    return {
        "added": added,
        "removed": removed,
        "modified": modified,
        "review_required": bool(removed or modified),
        "note": "Heuristic only. Verify actual compatibility with Schema Registry /compatibility API.",
    }


def transform_event(event: MetadataEvent) -> GraphMutation:
    p = event.payload
    evidence = _provenance(event)
    result = GraphMutation()

    if event.event_type == "schema_version_observed":
        subject = _name(p.get("subject"), "subject")
        topic = _name(p.get("topic"), "topic")
        version = int(p["version"])
        if version < 1:
            raise ValueError("schema version must be >= 1")
        schema = p["schema"]
        if not isinstance(schema, dict) or schema.get("type") != "record":
            raise ValueError("This lab expects an Avro record schema")
        sid, vid, tid = f"subject:{subject}", f"version:{subject}:{version}", f"topic:{topic}"
        result.entities.extend([
            Entity(id=sid, label="SchemaSubject", properties={"name": subject, "compatibility": p.get("compatibility", "UNKNOWN")}),
            Entity(id=vid, label="SchemaVersion", properties={"name": f"{subject}@v{version}", "version": version, "schema_id": int(p["schema_id"]), "fields": sorted(_fields(schema)), "schema_json": json.dumps(schema, sort_keys=True)}),
            Entity(id=tid, label="KafkaTopic", properties={"name": topic}),
        ])
        result.relationships.extend([
            Relationship(source_id=tid, target_id=sid, rel_type="USES_SUBJECT", properties=evidence),
            Relationship(source_id=sid, target_id=vid, rel_type="HAS_VERSION", properties=evidence),
        ])
        result.current_schema_version = (sid, vid, version)

    elif event.event_type == "schema_change_proposed":
        subject = _name(p.get("subject"), "subject")
        proposal_id = _name(p.get("proposal_id"), "proposal_id")
        current, proposed = p["current_schema"], p["proposed_schema"]
        assessment = assess_schema_change(current, proposed)
        sid, pid = f"subject:{subject}", f"proposal:{subject}:{proposal_id}"
        result.entities.extend([
            Entity(id=sid, label="SchemaSubject", properties={"name": subject}),
            Entity(id=pid, label="SchemaProposal", properties={"name": proposal_id, "status": "REVIEW_REQUIRED" if assessment["review_required"] else "REVIEW_RECOMMENDED", "removed_fields": assessment["removed"], "added_fields": assessment["added"], "modified_fields": assessment["modified"], "assessment_note": assessment["note"], "registry_compatible": p.get("registry_compatible")}),
        ])
        result.relationships.append(Relationship(source_id=pid, target_id=sid, rel_type="PROPOSES_CHANGE_TO", properties=evidence))

    elif event.event_type == "openlineage_run_event":
        job = p["job"]
        name = _name(job.get("name"), "job.name")
        namespace = _name(job.get("namespace"), "job.namespace")
        run_id = _name(p["run"]["runId"], "run.runId")
        status = _name(p.get("eventType"), "eventType")
        if status not in {"START", "RUNNING", "COMPLETE", "FAIL", "ABORT", "OTHER"}:
            raise ValueError(f"Unsupported OpenLineage eventType: {status}")
        pipeline_id, run_entity_id = f"pipeline:{name}", f"run:{namespace}:{name}:{run_id}"
        result.entities.extend([
            Entity(id=pipeline_id, label="Pipeline", properties={"name": name, "namespace": namespace}),
            Entity(id=run_entity_id, label="LineageRun", properties={"name": run_id, "status": status, "event_time": p.get("eventTime", event.occurred_at.isoformat())}),
        ])
        result.relationships.append(Relationship(source_id=run_entity_id, target_id=pipeline_id, rel_type="EXECUTES", properties=evidence))
        # Inputs/outputs observed on COMPLETE are materialized. FAIL is an execution fact,
        # not evidence that outputs were successfully produced.
        if status == "COMPLETE":
            for inp in p.get("inputs", []):
                dataset = _name(inp.get("name"), "input.name")
                if str(inp.get("namespace", "")).startswith("kafka://"):
                    iid, label, rel = f"topic:{dataset}", "KafkaTopic", "CONSUMES"
                else:
                    iid, label, rel = f"dataset:{dataset}", "Dataset", "READS_FROM"
                result.entities.append(Entity(id=iid, label=label, properties={"name": dataset}))
                result.relationships.append(Relationship(source_id=pipeline_id, target_id=iid, rel_type=rel, properties=evidence))
            for out in p.get("outputs", []):
                dataset = _name(out.get("name"), "output.name")
                oid = f"dataset:{dataset}"
                result.entities.append(Entity(id=oid, label="Dataset", properties={"name": dataset}))
                result.relationships.append(Relationship(source_id=pipeline_id, target_id=oid, rel_type="WRITES_TO", properties=evidence))

    elif event.event_type == "dbt_manifest_snapshot":
        manifest = p.get("manifest", p)
        # Real dbt manifest.json has nodes/sources. Simplified example manifest has models.
        if "nodes" in manifest:
            all_nodes = {**manifest.get("sources", {}), **manifest.get("nodes", {})}
            for unique_id, model in manifest["nodes"].items():
                if model.get("resource_type") != "model":
                    continue
                name = _name(model.get("name"), "model.name")
                model_id = f"dbt:{name}"
                output_name = model.get("relation_name") or f"{model.get('schema', 'gold')}.{model.get('alias') or name}"
                output_name = str(output_name).strip('"')
                input_names = []
                for dependency_id in model.get("depends_on", {}).get("nodes", []):
                    dep = all_nodes.get(dependency_id)
                    if dep is None:
                        continue
                    table = dep.get("relation_name") or f"{dep.get('schema', 'raw')}.{dep.get('alias') or dep.get('name', dependency_id)}"
                    input_names.append(str(table).strip('"'))
                _add_dbt_model(result, model_id, name, input_names, [output_name], evidence)
        elif "models" in manifest:
            for model in manifest["models"]:
                name = _name(model.get("name"), "model.name")
                _add_dbt_model(result, f"dbt:{name}", name, model.get("reads_from", []), model.get("produces", []), evidence)
        else:
            raise ValueError("dbt manifest must contain 'nodes' or 'models'")
    else:
        raise ValueError(f"Unsupported event: {event.event_type}")
    return result


def _add_dbt_model(result: GraphMutation, model_id: str, name: str, inputs: list[str], outputs: list[str], evidence: dict) -> None:
    result.entities.append(Entity(id=model_id, label="DbtModel", properties={"name": name}))
    desired: list[tuple[str, str]] = []
    for table in inputs:
        tid = f"dataset:{table}"
        result.entities.append(Entity(id=tid, label="Dataset", properties={"name": table}))
        result.relationships.append(Relationship(source_id=model_id, target_id=tid, rel_type="READS_FROM", properties=evidence))
        desired.append(("READS_FROM", tid))
    for table in outputs:
        tid = f"dataset:{table}"
        result.entities.append(Entity(id=tid, label="Dataset", properties={"name": table}))
        result.relationships.append(Relationship(source_id=model_id, target_id=tid, rel_type="PRODUCES", properties=evidence))
        desired.append(("PRODUCES", tid))
    result.reconcile_dbt_models[model_id] = desired
