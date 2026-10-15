"""Service methods for CLI and API; optional Kafka module imported on demand."""
from __future__ import annotations

from neo4j import GraphDatabase

from kg_lab.config import settings
from kg_lab.graph_loader import GraphLoader
from kg_lab.streaming.broker import consume_forever, publish
from kg_lab.streaming.demo import generate_demo_events
from kg_lab.streaming.impact import impact_analysis
from kg_lab.streaming.registry import SchemaRegistryClient


def _connect():
    return GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))


def initialize() -> None:
    with _connect() as driver:
        GraphLoader(driver, settings.neo4j_database, settings.document_embedding_dimensions).ensure_schema()


def publish_demo(phase: str) -> list[str]:
    events = generate_demo_events(phase, SchemaRegistryClient(settings.schema_registry_url))
    for event in events:
        publish(settings.kafka_bootstrap_servers, event)
    return [event.event_id for event in events]


def run_consumer(max_messages: int = 0, max_idle_seconds: int = 0) -> dict[str, int]:
    with _connect() as driver:
        GraphLoader(driver, settings.neo4j_database, settings.document_embedding_dimensions).ensure_schema()
        return consume_forever(
            driver,
            settings.neo4j_database,
            settings.kafka_bootstrap_servers,
            group_id=settings.kafka_consumer_group,
            max_messages=max_messages,
            max_idle_seconds=max_idle_seconds,
        )


def get_impact(subject: str) -> dict:
    with _connect() as driver:
        return impact_analysis(driver, settings.neo4j_database, subject)
