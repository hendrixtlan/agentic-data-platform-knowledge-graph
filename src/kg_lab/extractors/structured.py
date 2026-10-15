import json
from pathlib import Path

from kg_lab.models import Entity, ExtractionBatch, Relationship


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def extract_kafka(path: Path) -> ExtractionBatch:
    payload = _read_json(path)
    batch = ExtractionBatch()
    for row in payload["topics"]:
        topic_id = f"topic:{row['name']}"
        schema_id = f"schema:{row['schema']}"
        team_id = f"team:{row['owner']}"
        batch.entities.extend(
            [
                Entity(id=topic_id, label="KafkaTopic", properties={"name": row["name"]}),
                Entity(
                    id=schema_id,
                    label="Schema",
                    properties={"name": row["schema"], "compatibility": row["compatibility"]},
                ),
                Entity(id=team_id, label="Team", properties={"name": row["owner"]}),
            ]
        )
        batch.relationships.extend(
            [
                Relationship(source_id=topic_id, target_id=schema_id, rel_type="USES_SCHEMA", properties={"source": "kafka_metadata", "confidence": 1.0}),
                Relationship(source_id=topic_id, target_id=team_id, rel_type="OWNED_BY", properties={"source": "kafka_metadata", "confidence": 1.0}),
            ]
        )
    return batch


def extract_pipelines(path: Path) -> ExtractionBatch:
    payload = _read_json(path)
    batch = ExtractionBatch()
    for row in payload["pipelines"]:
        pipeline_id = f"pipeline:{row['name']}"
        team_id = f"team:{row['owner']}"
        batch.entities.extend(
            [
                Entity(id=pipeline_id, label="Pipeline", properties={"name": row["name"], "engine": row["engine"]}),
                Entity(id=team_id, label="Team", properties={"name": row["owner"]}),
            ]
        )
        batch.relationships.append(Relationship(source_id=pipeline_id, target_id=team_id, rel_type="OWNED_BY", properties={"source": "pipeline_metadata", "confidence": 1.0}))
        for topic in row.get("consumes", []):
            batch.relationships.append(Relationship(source_id=pipeline_id, target_id=f"topic:{topic}", rel_type="CONSUMES", properties={"source": "pipeline_metadata", "confidence": 1.0}))
        for dataset in row.get("writes_to", []):
            dataset_id = f"dataset:{dataset}"
            layer = dataset.split(".", 1)[0] if "." in dataset else "unknown"
            batch.entities.append(Entity(id=dataset_id, label="Dataset", properties={"name": dataset, "layer": layer}))
            batch.relationships.append(Relationship(source_id=pipeline_id, target_id=dataset_id, rel_type="WRITES_TO", properties={"source": "pipeline_metadata", "confidence": 1.0}))
    return batch


def extract_dbt(path: Path) -> ExtractionBatch:
    payload = _read_json(path)
    batch = ExtractionBatch()
    for row in payload["models"]:
        model_id = f"dbt:{row['name']}"
        team_id = f"team:{row['owner']}"
        batch.entities.extend(
            [
                Entity(id=model_id, label="DbtModel", properties={"name": row["name"]}),
                Entity(id=team_id, label="Team", properties={"name": row["owner"]}),
            ]
        )
        batch.relationships.append(Relationship(source_id=model_id, target_id=team_id, rel_type="OWNED_BY", properties={"source": "dbt_manifest", "confidence": 1.0}))
        for dataset in row.get("reads_from", []):
            dataset_id = f"dataset:{dataset}"
            batch.entities.append(Entity(id=dataset_id, label="Dataset", properties={"name": dataset, "layer": dataset.split(".", 1)[0]}))
            batch.relationships.append(Relationship(source_id=model_id, target_id=dataset_id, rel_type="READS_FROM", properties={"source": "dbt_manifest", "confidence": 1.0}))
        for dataset in row.get("produces", []):
            dataset_id = f"dataset:{dataset}"
            batch.entities.append(Entity(id=dataset_id, label="Dataset", properties={"name": dataset, "layer": dataset.split(".", 1)[0]}))
            batch.relationships.append(Relationship(source_id=model_id, target_id=dataset_id, rel_type="PRODUCES", properties={"source": "dbt_manifest", "confidence": 1.0}))
    return batch


def extract_bi(path: Path) -> ExtractionBatch:
    payload = _read_json(path)
    batch = ExtractionBatch()
    for row in payload["dashboards"]:
        dashboard_id = f"dashboard:{row['name']}"
        team_id = f"team:{row['owner']}"
        batch.entities.extend(
            [
                Entity(id=dashboard_id, label="Dashboard", properties={"name": row["name"], "tool": row["tool"]}),
                Entity(id=team_id, label="Team", properties={"name": row["owner"]}),
            ]
        )
        batch.relationships.append(Relationship(source_id=dashboard_id, target_id=team_id, rel_type="OWNED_BY", properties={"source": "bi_metadata", "confidence": 1.0}))
        for dataset in row.get("reads_from", []):
            dataset_id = f"dataset:{dataset}"
            batch.entities.append(Entity(id=dataset_id, label="Dataset", properties={"name": dataset, "layer": dataset.split(".", 1)[0]}))
            batch.relationships.append(Relationship(source_id=dataset_id, target_id=dashboard_id, rel_type="CONSUMED_BY", properties={"source": "bi_metadata", "confidence": 1.0}))
    return batch


def merge_batches(*batches: ExtractionBatch) -> ExtractionBatch:
    entities = {}
    relationships = {}
    for batch in batches:
        for entity in batch.entities:
            entities[entity.id] = entity
        for rel in batch.relationships:
            key = (rel.source_id, rel.target_id, rel.rel_type)
            relationships[key] = rel
    return ExtractionBatch(entities=list(entities.values()), relationships=list(relationships.values()))


def extract_all_structured(data_dir: Path) -> ExtractionBatch:
    return merge_batches(
        extract_kafka(data_dir / "kafka_metadata.json"),
        extract_pipelines(data_dir / "pipeline_metadata.json"),
        extract_dbt(data_dir / "dbt_manifest.json"),
        extract_bi(data_dir / "bi_metadata.json"),
    )
