# ADR-002: Event-driven metadata ingestion for customer_events and Customer360

Status: Accepted for research lab, not an enterprise production approval.

Decision: Store Schema Registry subjects and immutable accepted versions separately from proposed changes. Publish metadata observations and OpenLineage run events to a dedicated Kafka metadata topic. Project them into Neo4j with atomic `AppliedEvent` inbox and Cypher `MERGE`, committing the Kafka offset only after Neo4j succeeds. Use Neo4j graph traversal to identify dependent pipelines, datasets and Power BI dashboards. Keep document RAG for design rationale and LangGraph for retrieval orchestration.

Reason: A vector-only retriever is not a complete lineage system. We need structured, multi-hop evidence that a schema subject feeds customer_events, CustomerStreaming, bronze.customer, a dbt model and gold.customer_360 before Customer Executive Dashboard. The schema compatibility API is authoritative for Avro compatibility, while downstream impact may still require a human risk assessment. Kafka makes projection asynchronous, recoverable and replayable.

Tradeoffs: The local lab uses synthetic lineage and dbt data, deterministic hash embeddings, a single Neo4j database and at-least-once Kafka delivery with exactly-once graph effect per event ID. For enterprise use, add authentication, credentials vault, owner ACLs, entity resolution across projects, distributed checkpointing, tracing and semantics evaluation.
