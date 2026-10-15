import json

import typer

from kg_lab.langgraph_app import agent
from kg_lab.pipeline import seed_graph

app = typer.Typer(help="Knowledge Graph + RAG + LangGraph lab with event-driven ingestion")


@app.command()
def seed():
    """Extract sample metadata/docs and load Neo4j."""
    counts = seed_graph()
    typer.echo(json.dumps(counts, indent=2))


@app.command()
def ask(question: str):
    """Ask the LangGraph-orchestrated agent."""
    result = agent.invoke({"question": question})
    typer.echo(result["answer"])


@app.command("stream-consume")
def stream_consume(max_messages: int = 0, max_idle_seconds: int = 0):
    """Consume metadata events and atomically project them into Neo4j."""
    from kg_lab.streaming.service import run_consumer

    typer.echo(json.dumps(run_consumer(max_messages, max_idle_seconds), indent=2))


@app.command("demo-publish")
def demo_publish(phase: str = typer.Option("baseline", help="baseline or changed")):
    """Register schemas and publish a change-impact scenario."""
    from kg_lab.streaming.service import publish_demo

    typer.echo(json.dumps({"event_ids": publish_demo(phase)}, indent=2))


@app.command("registry-watch")
def registry_watch(
    subject: str = "customer_events-value",
    topic: str = "customer_events",
    once: bool = False,
    interval: int = 10,
):
    """Poll Schema Registry and publish observed new versions to Kafka."""
    from neo4j import GraphDatabase
    from kg_lab.config import settings
    from kg_lab.streaming.registry import SchemaRegistryClient
    from kg_lab.streaming.watch import watch_forever

    with GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password)) as driver:
        published = watch_forever(
            driver, settings.neo4j_database,
            SchemaRegistryClient(settings.schema_registry_url),
            settings.kafka_bootstrap_servers, subject, topic,
            interval_seconds=interval, once=once,
        )
    if once:
        typer.echo(json.dumps({"published": published}))


@app.command("impact")
def impact(subject: str = "customer_events-value"):
    """Structured schema-change impact: Registry + lineage + dashboards."""
    from kg_lab.streaming.service import get_impact

    typer.echo(json.dumps(get_impact(subject), indent=2))


if __name__ == "__main__":
    app()
