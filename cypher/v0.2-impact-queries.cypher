// Inspect accepted schema history: show version nodes, not just CURRENT_VERSION.
MATCH (s:SchemaSubject {name:'customer_events-value'})-[:HAS_VERSION]->(v:SchemaVersion)
RETURN s.name AS subject, v.version AS version, v.fields AS fields
ORDER BY version;

// Latest accepted version points to exactly one version (monotonic update).
MATCH (s:SchemaSubject {name:'customer_events-value'})-[:CURRENT_VERSION]->(v:SchemaVersion)
RETURN s.name, v.version, s.compatibility;

// Proposed change is NOT an accepted schema version.
MATCH (p:SchemaProposal)-[:PROPOSES_CHANGE_TO]->(s:SchemaSubject)
RETURN s.name, p.name, p.status, p.registry_compatible,
       p.removed_fields, p.modified_fields;

// Impact: which dashboards rely on a topic's downstream lineage?
MATCH path=(s:SchemaSubject {name:'customer_events-value'})
           <-[:USES_SUBJECT]-(topic:KafkaTopic)
           <-[:CONSUMES]-(job:Pipeline)
           -[:WRITES_TO]->(bronze:Dataset)
           <-[:READS_FROM]-(model:DbtModel)
           -[:PRODUCES]->(gold:Dataset)
           -[:CONSUMED_BY]->(dashboard:Dashboard)
RETURN path;

// Lifecycle, provenance and projected event count.
MATCH (e:AppliedEvent)
RETURN e.source AS source, e.event_type AS kind, count(*) AS processed
ORDER BY processed DESC;

// Completed/failed runs and their owners / jobs.
MATCH (run:LineageRun)-[:EXECUTES]->(job:Pipeline)
RETURN run.name AS run_id, run.status AS state, job.name AS job
ORDER BY run.event_time DESC;
