"""Bounded, parameterized impact query. Read-only and without model-authored Cypher."""
from __future__ import annotations

from typing import Any


IMPACT_QUERY = """
MATCH (s:SchemaSubject {name: $subject})
OPTIONAL MATCH (topic:KafkaTopic)-[:USES_SUBJECT]->(s)
OPTIONAL MATCH (p:Pipeline)-[:CONSUMES]->(topic)
OPTIONAL MATCH (p)-[:WRITES_TO]->(bronze:Dataset)
OPTIONAL MATCH (m:DbtModel)-[:READS_FROM]->(bronze)
OPTIONAL MATCH (m)-[:PRODUCES]->(gold:Dataset)
OPTIONAL MATCH (gold)-[:CONSUMED_BY]->(dashboard:Dashboard)
WITH s,
     collect(DISTINCT topic.name) AS topics,
     collect(DISTINCT p.name) AS pipelines,
     collect(DISTINCT bronze.name) AS landing_datasets,
     collect(DISTINCT m.name) AS models,
     collect(DISTINCT gold.name) AS output_datasets,
     collect(DISTINCT dashboard.name) AS dashboards
OPTIONAL MATCH (s)-[:CURRENT_VERSION]->(v:SchemaVersion)
OPTIONAL MATCH (proposal:SchemaProposal)-[:PROPOSES_CHANGE_TO]->(s)
RETURN s.name AS subject,
       s.compatibility AS compatibility,
       v.version AS current_version,
       topics, pipelines, landing_datasets, models, output_datasets, dashboards,
       collect(DISTINCT {
         id: proposal.name,
         status: proposal.status,
         registry_compatible: proposal.registry_compatible,
         removed_fields: proposal.removed_fields,
         modified_fields: proposal.modified_fields
       }) AS proposals
"""


def impact_analysis(driver, database: str, subject: str) -> dict[str, Any]:
    if not subject.strip():
        raise ValueError("subject must not be empty")
    with driver.session(database=database) as session:
        row = session.execute_read(
            lambda tx: tx.run(IMPACT_QUERY, subject=subject).single()
        )
    if row is None:
        return {"subject": subject, "found": False, "message": "No subject found in graph"}
    result = dict(row)
    result["found"] = True
    result["proposals"] = [p for p in result["proposals"] if p["id"] is not None]
    result["scope"] = "Demo impact query: Kafka → streaming job → bronze → dbt → gold → dashboard"
    return result
