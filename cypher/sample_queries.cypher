// 1. Full local neighborhood around customer_events
MATCH p=(t:KafkaTopic {name: 'customer_events'})-[*1..5]-(x)
RETURN p;

// 2. Structured lineage from Kafka to Power BI
MATCH p=(t:KafkaTopic {name: 'customer_events'})
  <-[:CONSUMES]-(pipeline:Pipeline)
  -[:WRITES_TO]->(bronze:Dataset)
  <-[:READS_FROM]-(model:DbtModel)
  -[:PRODUCES]->(gold:Dataset)
  -[:CONSUMED_BY]->(dashboard:Dashboard)
RETURN p;

// 3. Owners in the impact radius
MATCH (t:KafkaTopic {name: 'customer_events'})-[*0..5]-(component)
MATCH (component)-[:OWNED_BY]->(team:Team)
RETURN DISTINCT component.name AS component, team.name AS owner
ORDER BY component;

// 4. Documentation linked to customer_events
MATCH (d:DocumentChunk)-[:MENTIONS]->(t:KafkaTopic {name: 'customer_events'})
RETURN d.source, d.text;
