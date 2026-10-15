# Changelog

## v0.2.0 — Event-driven schema intelligence

- Retained the v0.1 batch ETL, vector document index, Cypher retrieval, LangGraph router, CLI and FastAPI.
- Added a Kafka-compatible Redpanda broker and Confluent-compatible Schema Registry, with optional web console and containerized API/consumer.
- Added validated JSON metadata envelopes, a Schema Registry watcher, actual REST schema registration/compatibility checks, and OpenLineage/dbt manifest adapters.
- Added an atomic Neo4j inbox (`AppliedEvent`) for replay idempotence, a dead-letter path for invalid events, monotonic current schema version and event provenance.
- Added accepted-version history, non-registered proposed changes, bounded schema impact into BI, Cypher exploration queries and a new FastAPI impact endpoint.
- Added source-only tests of replay, schema diffs, event mappings and consumer error semantics, plus Compose-based end-to-end integration workflow.
- The sample OpenLineage/dbt artifacts and docs remain synthetic, and Docker integration has not been executed in the offline authoring environment.

## v0.1.0 — Knowledge Graph + GraphRAG baseline

- Synthetic metadata ETL to Neo4j with deterministic, local document index and LangGraph hybrid retrieval.
