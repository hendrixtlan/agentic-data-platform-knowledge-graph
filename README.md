# Agentic Data Platform Knowledge Graph — v0.2

**Event-driven Neo4j + Kafka/Redpanda + Schema Registry + OpenLineage + dbt + LangGraph + GraphRAG.** This is an executable extension of the v0.1 batch and RAG lab, with a real local Kafka-compatible broker, accepted Registry versions, atomic graph projection and change-impact analysis into Power BI.

## Start here: end-to-end scenario

Prerequisites: Docker Compose, Python 3.11+ and ~4 GB free memory. On Windows, Docker Desktop + WSL2 is recommended. From the repository root:

```bash
python -m venv .venv
# Linux/macOS: source .venv/bin/activate
# PowerShell: .\.venv\Scripts\Activate.ps1
pip install -e '.[dev,stream]'
docker compose up -d --wait
kg-lab seed
kg-lab demo-publish --phase baseline
kg-lab demo-publish --phase changed
kg-lab stream-consume --max-messages 5
kg-lab impact customer_events-value
kg-lab ask "What is impacted by changes to customer_events-value and why was Kafka selected?"
RUN_INTEGRATION=1 pytest -q -m integration
```

PowerShell integration-test equivalent: `$env:RUN_INTEGRATION="1"; pytest -q -m integration`. The batch seed creates the original historical Kafka → pipeline → dbt → Power BI graph and document index. The baseline phase registers Avro v1 in actual Schema Registry and publishes OpenLineage and dbt metadata. The changed phase registers an accepted, backward-compatible v2 and publishes a proposed (not registered) v3 change that the Registry compatibility endpoint evaluates. Five events are consumed. Inspect `http://localhost:7474` (Neo4j Browser), and optionally `docker compose --profile ui up -d` for Redpanda Console at `http://localhost:8080`.

**Keep credentials out of Git:** the lab credentials and exposed ports are for isolated localhost development only, never internet-exposed servers. Copy `.env.example` to `.env` before changing credentials and never commit it.

### Completely containerized mode (no local Python installation)

```bash
docker compose --profile app up -d --build --wait
docker compose run --rm consumer kg-lab seed
docker compose run --rm consumer kg-lab demo-publish --phase baseline
docker compose run --rm consumer kg-lab demo-publish --phase changed
docker compose run --rm consumer kg-lab impact customer_events-value
```

The long-running consumer and FastAPI service are started by the `app` profile; inspect `http://localhost:8000/impact/customer_events-value` and `http://localhost:8000/docs`. The API's graph/doc mode uses deterministic local embeddings and needs no model key. For optional OpenAI-based synthesis use `pip install -e '.[llm]'` in host Python and set `OPENAI_API_KEY` locally; the minimal Docker image does not include this optional extra.

### Registry watcher: source-driven discovery

The CLI demo explicitly publishes events after registering schemas, but a real integration must detect changes without someone running a demo. Start `kg-lab registry-watch --interval 10` against the running stack to poll Registry, backfill unobserved accepted versions, and publish them to Kafka. `--once` performs a single scan. Do **not** run it during the fixed five-event CI scenario: it can publish additional valid observation events. A small polling interval is not equivalent to push-based Registry notifications. Configure subject/topic names with `--subject` and `--topic`.

## Research questions and evidence

`kg-lab impact customer_events-value` returns accepted version, Kafka topics, pipelines, bronze datasets, dbt models, gold outputs, dashboards and proposed changes with Registry compatibility. `cypher/v0.2-impact-queries.cypher` contains inspectable statements. `kg-lab ask` uses LangGraph to route graph/doc/hybrid retrieval and can include the fixed, parameterized impact tool. No arbitrary LLM-generated write Cypher is executed. Use the `AppliedEvent` nodes and consumer counters to study replay and reliability.

**Demonstrated / not demonstrated:** This repo includes a real local Schema Registry REST integration and Kafka API stream, but synthetic OpenLineage and dbt fixture inputs, not running Airflow or dbt in production. Its hash embedding retrieval is deterministic for teaching, not a claim of strong semantic search. The exact semantics, failure boundaries and remaining gaps are described in [the v0.2 architecture](docs/v0.2-architecture.md), [testing guide](docs/testing-guide.md) and [English presentation brief](docs/presentation-brief-en.md).

## Source layout (incremental additions)

`src/kg_lab/streaming/{events,transform,apply,broker,registry,watch,impact,demo,service}.py` contains envelopes, source adapters, canonical mutations, atomic Neo4j projection, Kafka consumers, Registry watcher and impact query. `data/events/` contains Avro and OpenLineage/dbt fixtures. CI runs unit and Kafka/Neo4j integration jobs; Docker Compose runs Neo4j, Redpanda, optional console and optional API/consumer workers. **APOC is not required.**

---

## Original v0.1 lab (retained below)

# Agentic Data Platform Knowledge Graph

An end-to-end professional lab that turns data-platform metadata and technical documentation into a **Neo4j Knowledge Graph**, retrieves both **graph relationships** and **semantic document context**, and uses **LangGraph** to orchestrate the answer path.

The use case is intentionally practical: **data lineage, change-impact analysis, ownership, architecture reasoning, and GraphRAG**.

## What this repository proves

A Knowledge Graph should not be hand-built node by node. The repository automates the path from source metadata into a graph:

```text
Kafka / pipeline / dbt / BI metadata             Architecture docs / runbooks
                 |                                            |
                 v                                            v
       deterministic extraction                    chunk + embed + entity link
                 |                                            |
                 +-----------------+--------------------------+
                                   v
                      Canonical graph data model
                                   |
                                   v
                                Neo4j
                     /                         \
              Cypher graph retrieval       vector retrieval
                     \                         /
                      +-------- LangGraph -----+
                                   |
                              evidence synthesis
                                   |
                              CLI / FastAPI
```

The repository is **Cypher-first**. APOC is not required for the main workflow.

## Domain model

The sample graph models a data platform with these relationships:

```text
CustomerEventV4 <-[:USES_SCHEMA]- customer_events
                                      ^
                                      |
                                  [:CONSUMES]
                                      |
                               CustomerStreaming
                                      |
                                 [:WRITES_TO]
                                      v
                               bronze.customer
                                      ^
                                      |
                                [:READS_FROM]
                                      |
                                  customer_360
                                      |
                                  [:PRODUCES]
                                      v
                              gold.customer_360
                                      |
                                [:CONSUMED_BY]
                                      v
                         Customer Executive Dashboard
```

Technical documents are stored as `DocumentChunk` nodes with embeddings and `MENTIONS` links to graph entities.

## Why LangGraph and Neo4j belong together

Neo4j answers questions about connected facts: dependency chains, ownership, lineage, impact radius, and paths. LangGraph does not replace Neo4j; it **orchestrates when Neo4j should be queried** and when document retrieval should be added.

For example:

```text
Question: "What systems depend on customer_events and why was Kafka selected?"

LangGraph router
     |
     +--> Graph retriever --> Neo4j / Cypher --> downstream lineage
     |
     +--> Vector retriever -> Neo4j vector index --> ADR / runbook evidence
     |
     +--> Synthesis --> grounded answer
```

## Quick start

### 1. Start Neo4j

```bash
docker compose up -d
```

Neo4j Browser: `http://localhost:7474`

Credentials:

```text
user: neo4j
password: change-me-now
```

### 2. Create a Python environment

```bash
python -m venv .venv
```

Linux/macOS:

```bash
source .venv/bin/activate
```

PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Install

```bash
pip install -e '.[dev]'
```

Copy the environment template:

```bash
cp .env.example .env
```

PowerShell:

```powershell
Copy-Item .env.example .env
```

### 4. Seed the graph

```bash
kg-lab seed
```

This performs the complete ETL: extract sample metadata, normalize it into entities and relationships, create constraints/vector index, load Neo4j, chunk documentation, generate local embeddings, and link documents to known entities.

### 5. Run Cypher directly

Open Neo4j Browser and try:

```cypher
MATCH p=(t:KafkaTopic {name: 'customer_events'})
  <-[:CONSUMES]-(pipeline:Pipeline)
  -[:WRITES_TO]->(bronze:Dataset)
  <-[:READS_FROM]-(model:DbtModel)
  -[:PRODUCES]->(gold:Dataset)
  -[:CONSUMED_BY]->(dashboard:Dashboard)
RETURN p;
```

More examples are in `cypher/sample_queries.cypher`.

### 6. Ask the LangGraph agent

```bash
kg-lab ask "What systems depend on customer_events and why was Kafka selected?"
```

No LLM key is required. In local mode, the agent returns the retrieved graph and document evidence so you can inspect exactly what the system found.

### 7. Optional: enable generative synthesis

```bash
pip install -e '.[llm]'
```

Set these values in `.env`:

```text
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-5
```

Then repeat the same `kg-lab ask` command. LangGraph uses the same retrieval flow but the final evidence is synthesized by the configured model.

### 8. Run the API

```bash
uvicorn kg_lab.api:app --reload --port 8000
```

Request:

```bash
curl -X POST http://localhost:8000/ask \
  -H 'Content-Type: application/json' \
  -d '{"question":"What systems depend on customer_events and why?"}'
```

## Repository structure

```text
src/kg_lab/
  extractors/          source metadata + document extraction
  retrieval/           graph and vector retrievers
  langgraph_app/       routing/orchestration workflow
  graph_loader.py      idempotent Neo4j MERGE loading
  hash_embedder.py     local deterministic demo embeddings
  pipeline.py          end-to-end seed pipeline
  api.py               FastAPI
  cli.py               command-line interface

data/                  synthetic platform metadata and docs
cypher/                 constraints and research queries
docs/                   architecture and research experiments
tests/                  unit tests
.github/workflows/      CI
```

## ETL design principle

The source systems do not write Cypher directly. Every extractor first maps its source into a canonical model:

```python
Entity(id, label, properties)
Relationship(source_id, target_id, rel_type, properties)
```

The graph loader owns the Neo4j mapping. This keeps source adapters independent from the graph persistence layer and makes it straightforward to add Kafka APIs, Schema Registry, Airflow, OpenLineage, Fabric, Databricks, Snowflake, Git, or Jira later.

## Provenance

Every deterministic sample relationship carries metadata such as:

```text
source = dbt_manifest
confidence = 1.0
```

For a later LLM-based document extractor, store a lower confidence plus extraction method and source document. This allows authoritative metadata and inferred knowledge to coexist without treating them as equally trustworthy.

## Production evolution

The current lab is deliberately small and explainable. A production-oriented next version would add:

1. real Kafka and Schema Registry extractors;
2. OpenLineage or Airflow lineage events;
3. incremental graph updates rather than full reseeding;
4. entity resolution across platform naming systems;
5. production embeddings and reranking;
6. authorization-aware graph retrieval;
7. OpenTelemetry traces across LangGraph nodes;
8. evaluation datasets for graph-only vs vector-only vs hybrid retrieval;
9. human approval before any agent writes back to enterprise systems.

## Current technology choices

- Neo4j 5.26 LTS line for a stable lab baseline.
- Cypher for constraints, loading, lineage, and graph retrieval.
- Neo4j vector index for document retrieval.
- LangGraph for stateful routing and orchestration.
- FastAPI for an application boundary.
- Optional `neo4j-graphrag` dependency group for extending the lab with Neo4j's first-party GraphRAG package; keep provider-specific embeddings in a separate matching-dimension index.

## Research exercises

Start with three questions and compare retrieval modes:

```text
1. What is downstream of customer_events?          -> graph strength
2. Why was Kafka selected for Customer360?         -> document RAG strength
3. What systems depend on customer_events and why? -> hybrid strength
```

That comparison is the core learning objective: **graphs are strongest for explicit connected facts, vector retrieval is strongest for semantic documentary context, and LangGraph provides the control plane that decides how to combine them.**

## Integration test

After Neo4j is running and the project dependencies are installed:

```bash
RUN_INTEGRATION=1 pytest -q -m integration
```

PowerShell:

```powershell
$env:RUN_INTEGRATION="1"; pytest -q -m integration
```

This seeds Neo4j and verifies both a Cypher graph retrieval and a vector document retrieval.

## Publish to GitHub

```bash
git init
git add .
git commit -m "Initial end-to-end Knowledge Graph + GraphRAG lab"
git branch -M main
git remote add origin <your-repository-url>
git push -u origin main
```
