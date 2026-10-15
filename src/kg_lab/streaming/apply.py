"""Atomic, idempotent event projection into Neo4j; Cypher-first (no APOC)."""
from __future__ import annotations

from collections import defaultdict

from kg_lab.graph_schema import ALLOWED_LABELS, ALLOWED_RELATIONSHIPS
from kg_lab.streaming.events import MetadataEvent
from kg_lab.streaming.transform import GraphMutation, transform_event


class EventConflict(ValueError):
    """A previously applied event_id was reused with different content."""


def _write_transaction(tx, event: MetadataEvent, mutation: GraphMutation) -> str:
    """All projections AND the inbox checkpoint commit as one Neo4j transaction."""
    digest = event.fingerprint()
    record = tx.run(
        """
        MERGE (e:AppliedEvent {id: $event_id})
        ON CREATE SET e.status = 'PENDING', e.fingerprint = $fingerprint
        RETURN e.status AS status, e.fingerprint AS fingerprint
        """,
        event_id=event.event_id,
        fingerprint=digest,
    ).single()
    if record["fingerprint"] != digest:
        raise EventConflict(f"Conflicting payload for event_id: {event.event_id}")
    if record["status"] == "APPLIED":
        return "duplicate"

    grouped_entities = defaultdict(list)
    for entity in mutation.entities:
        if entity.label not in ALLOWED_LABELS:
            raise ValueError(f"Disallowed entity label: {entity.label}")
        grouped_entities[entity.label].append({"id": entity.id, "properties": entity.properties})
    for label, rows in grouped_entities.items():
        if label == "LineageRun":
            # Never let a late START overwrite an already-observed COMPLETE/FAIL.
            tx.run(
                """
                UNWIND $rows AS row
                MERGE (n:LineageRun {id: row.id})
                WITH n, row
                WHERE n.event_time IS NULL OR datetime(n.event_time) <= datetime(row.properties.event_time)
                SET n += row.properties, n.updated_at = datetime()
                """, rows=rows,
            ).consume()
            continue
        tx.run(
            f"""
            UNWIND $rows AS row
            MERGE (n:{label} {{id: row.id}})
            SET n += row.properties, n.updated_at = datetime()
            """,
            rows=rows,
        ).consume()

    # A new dbt manifest is authoritative for links created by dbt, not other sources.
    for model_id, desired in mutation.reconcile_dbt_models.items():
        tx.run(
            """
            MATCH (m:DbtModel {id: $model_id})-[r:READS_FROM|PRODUCES]->(d:Dataset)
            WHERE r.source IN ['dbt_manifest', 'dbt_artifact']
              AND NOT [type(r), d.id] IN $desired
            DELETE r
            """,
            model_id=model_id,
            desired=[list(pair) for pair in desired],
        ).consume()

    grouped_rels = defaultdict(list)
    for rel in mutation.relationships:
        if rel.rel_type not in ALLOWED_RELATIONSHIPS:
            raise ValueError(f"Disallowed relationship: {rel.rel_type}")
        grouped_rels[rel.rel_type].append(rel.model_dump())
    for rel_type, rows in grouped_rels.items():
        tx.run(
            f"""
            UNWIND $rows AS row
            MATCH (a {{id: row.source_id}})
            MATCH (b {{id: row.target_id}})
            MERGE (a)-[r:{rel_type}]->(b)
            SET r += row.properties, r.updated_at = datetime()
            """,
            rows=rows,
        ).consume()

    if mutation.current_schema_version is not None:
        subject_id, version_id, version = mutation.current_schema_version
        tx.run(
            """
            MATCH (s:SchemaSubject {id: $subject_id})
            MATCH (v:SchemaVersion {id: $version_id})
            WHERE coalesce(s.latest_version, 0) < $version
            OPTIONAL MATCH (s)-[old:CURRENT_VERSION]->(:SchemaVersion)
            DELETE old
            MERGE (s)-[:CURRENT_VERSION]->(v)
            SET s.latest_version = $version,
                s.last_change_at = $occurred_at
            """,
            subject_id=subject_id,
            version_id=version_id,
            version=version,
            occurred_at=event.occurred_at.isoformat(),
        ).consume()

    tx.run(
        """
        MATCH (e:AppliedEvent {id: $event_id})
        SET e.status = 'APPLIED',
            e.event_type = $event_type,
            e.source = $source,
            e.occurred_at = $occurred_at,
            e.applied_at = datetime()
        """,
        event_id=event.event_id,
        event_type=event.event_type,
        source=event.source,
        occurred_at=event.occurred_at.isoformat(),
    ).consume()
    return "applied"


def apply_event(driver, database: str, event: MetadataEvent, mutation: GraphMutation | None = None) -> str:
    """Returns applied/duplicate. Invalid events or Neo4j failures raise.

    The Kafka consumer must commit offsets ONLY after this returns successfully.
    """
    mutation = mutation if mutation is not None else transform_event(event)
    # Validate/transform completely before opening a transaction.
    with driver.session(database=database) as session:
        return session.execute_write(_write_transaction, event, mutation)
