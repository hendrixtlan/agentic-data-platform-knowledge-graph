# Research notes

## Research questions

1. When does graph retrieval outperform vector-only retrieval for lineage and impact analysis?
2. What metadata should be treated as authoritative versus LLM-extracted?
3. How should entity resolution work across Kafka, dbt, orchestration, BI, and documentation?
4. How should provenance and confidence be represented on relationships?
5. How much graph traversal depth is useful before context becomes noisy?
6. When should LangGraph route directly to Cypher versus hybrid Graph + Vector retrieval?

## Suggested experiments

- Remove the graph path and compare answers for dependency questions.
- Remove vector retrieval and compare answers for architectural "why" questions.
- Introduce duplicate entity names and implement deterministic entity resolution.
- Add OpenLineage events and update Neo4j continuously rather than through batch seed data.
- Add Kafka change events so schema changes trigger incremental graph updates.
- Replace the local hashing embedder with a production embedding model and measure retrieval quality.
- Add a human approval node in LangGraph before any write/action tool is invoked.
