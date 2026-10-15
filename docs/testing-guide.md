# Testing and validation

Unit tests (`pytest -q -m 'not integration'`) cover envelopes, event partition keys, schema-change field diff, transformation of OpenLineage events, dbt dependency resolution, Registry watcher catch-up, simulated atomic replay and consumer offset/DLQ behavior. These tests do not need Docker.

Integration tests (`RUN_INTEGRATION=1 pytest -q -m integration`) require the full lab to be initialized and the five demo events projected first; they run Neo4j queries for actual subjects, schema version, proposal, pipeline and Power BI impact.

**CI** has two jobs: `unit` for local logic and lint, and `integration` for Docker Compose, Schema Registry registration, Kafka publish/consume, Neo4j projection and assertions. If the integration job fails in a hosted CI environment, inspect Compose logs. No full-container smoke test can be claimed until that job has actually run.

**Repeatability**: `docker compose down -v` removes local Neo4j and Registry/Kafka state. Running an unchanged schema registration against an existing Registry does not necessarily create a new version. For a deterministic five-event smoke test either reset volumes or run against a fresh consumer group and understand already-applied events will appear as duplicates.
