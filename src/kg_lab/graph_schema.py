ALLOWED_LABELS = {
    "KafkaTopic",
    "Schema",
    "Pipeline",
    "Dataset",
    "DbtModel",
    "Dashboard",
    "Team",
    "SchemaSubject",
    "SchemaVersion",
    "SchemaProposal",
    "LineageRun",
}

ALLOWED_RELATIONSHIPS = {
    "USES_SCHEMA",
    "OWNED_BY",
    "CONSUMES",
    "WRITES_TO",
    "READS_FROM",
    "PRODUCES",
    "CONSUMED_BY",
    "MENTIONS",
    "USES_SUBJECT",
    "HAS_VERSION",
    "CURRENT_VERSION",
    "PROPOSES_CHANGE_TO",
    "EXECUTES",
}

CONSTRAINTS = [
    "CREATE CONSTRAINT kafka_topic_id IF NOT EXISTS FOR (n:KafkaTopic) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT schema_id IF NOT EXISTS FOR (n:Schema) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT pipeline_id IF NOT EXISTS FOR (n:Pipeline) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT dataset_id IF NOT EXISTS FOR (n:Dataset) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT dbt_model_id IF NOT EXISTS FOR (n:DbtModel) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT dashboard_id IF NOT EXISTS FOR (n:Dashboard) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT team_id IF NOT EXISTS FOR (n:Team) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT document_chunk_id IF NOT EXISTS FOR (n:DocumentChunk) REQUIRE n.id IS UNIQUE",
]

CONSTRAINTS += [
    "CREATE CONSTRAINT schema_subject_id IF NOT EXISTS FOR (n:SchemaSubject) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT schema_version_id IF NOT EXISTS FOR (n:SchemaVersion) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT schema_proposal_id IF NOT EXISTS FOR (n:SchemaProposal) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT lineage_run_id IF NOT EXISTS FOR (n:LineageRun) REQUIRE n.id IS UNIQUE",
    "CREATE CONSTRAINT applied_event_id IF NOT EXISTS FOR (n:AppliedEvent) REQUIRE n.id IS UNIQUE",
]
