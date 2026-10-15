"""Schema Registry connector: poll for accepted versions and publish new ones.

Polling is intentional: Schema Registry REST is not itself a Kafka event stream.
Neo4j's version pointer is the checkpoint. Re-emission is safe with projection
idempotence and monotonic CURRENT_VERSION updates.
"""
from __future__ import annotations

import json
import logging
import time

from kg_lab.streaming.events import make_event
from kg_lab.streaming.registry import SchemaRegistryClient

logger = logging.getLogger(__name__)


def discover_versions(registry: SchemaRegistryClient, subject: str, topic: str,
                      last_seen_version: int) -> list:
    latest = int(registry.version(subject)["version"])
    result = []
    for version in range(last_seen_version + 1, latest + 1):
        row = registry.version(subject, str(version))
        event = make_event(
            "schema_version_observed",
            "schema_registry_watcher",
            {
                "subject": subject,
                "topic": topic,
                "version": int(row["version"]),
                "schema_id": int(row["id"]),
                "compatibility": registry.compatibility(subject),
                "schema": json.loads(row["schema"]),
            },
            natural_key=f"{subject}:{version}",
        )
        result.append(event)
    return result


def poll_once(driver, database: str, registry: SchemaRegistryClient, bootstrap: str,
              subject: str, topic: str) -> int:
    from kg_lab.streaming.broker import publish

    with driver.session(database=database) as session:
        record = session.run(
            "MATCH (s:SchemaSubject {id:$id}) RETURN s.latest_version AS version",
            id=f"subject:{subject}",
        ).single()
    last_seen_version = int(record["version"]) if record and record["version"] else 0
    events = discover_versions(registry, subject, topic, last_seen_version)
    for event in events:
        publish(bootstrap, event)
    logger.info("schema_watcher subject=%s published=%d", subject, len(events))
    return len(events)


def watch_forever(driver, database: str, registry: SchemaRegistryClient, bootstrap: str,
                  subject: str, topic: str, interval_seconds: int = 10,
                  once: bool = False):
    if interval_seconds < 1:
        raise ValueError("interval_seconds must be >= 1")
    while True:
        count = poll_once(driver, database, registry, bootstrap, subject, topic)
        if once:
            return count
        time.sleep(interval_seconds)
