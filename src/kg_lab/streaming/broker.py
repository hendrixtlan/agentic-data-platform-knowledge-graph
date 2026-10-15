"""Kafka I/O adapter; Redpanda is Kafka API compatible. Imported lazily."""
from __future__ import annotations

import json
import logging
import time

from pydantic import ValidationError

from kg_lab.streaming.events import MetadataEvent, partition_key
from kg_lab.streaming.apply import apply_event
from kg_lab.streaming.transform import transform_event

log = logging.getLogger(__name__)
EVENTS_TOPIC = "kg.metadata.events"
DLQ_TOPIC = "kg.metadata.dlq"


def ensure_topics(bootstrap: str, topics: tuple[str, ...] = (EVENTS_TOPIC, DLQ_TOPIC)) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    futures = admin.create_topics([NewTopic(name, num_partitions=3, replication_factor=1) for name in topics if name not in existing])
    for topic, future in futures.items():
        future.result(timeout=20)
        log.info("topic_created topic=%s", topic)


def publish(bootstrap: str, event: MetadataEvent) -> None:
    from confluent_kafka import Producer

    ensure_topics(bootstrap)
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    errors = []
    def delivered(err, msg):
        if err is not None:
            errors.append(str(err))
    producer.produce(EVENTS_TOPIC, key=partition_key(event), value=event.to_json(), on_delivery=delivered)
    if producer.flush(15) != 0 or errors:
        raise RuntimeError(f"Event not confirmed delivered: {errors}")
    log.info("event_published id=%s kind=%s", event.event_id, event.event_type)


def _dlq(producer, msg, exc: Exception) -> None:
    envelope = {
        "error_type": type(exc).__name__,
        "error": str(exc)[:600],
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "original_payload": msg.value().decode("utf-8", errors="replace")[:200_000],
    }
    errors = []
    def delivered(err, _msg):
        if err is not None:
            errors.append(str(err))
    producer.produce(DLQ_TOPIC, key=msg.key(), value=json.dumps(envelope).encode(), on_delivery=delivered)
    if producer.flush(15) != 0 or errors:
        raise RuntimeError(f"DLQ publish not confirmed; refusing offset commit: {errors}")


def consume_forever(
    driver,
    database: str,
    bootstrap: str,
    group_id: str = "kg-projection-v2",
    max_messages: int = 0,
    max_idle_seconds: int = 0,
) -> dict[str, int]:
    """At-least-once delivery; exactly-once EFFECT per event_id in the graph.

    Bad envelopes go to a DLQ before offset commit. Unexpected projection/DB
    failures STOP the worker, preserving the offset for replay after recovery.
    """
    from confluent_kafka import Consumer, Producer

    ensure_topics(bootstrap)
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
    })
    dlq_producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    counts = {"applied": 0, "duplicate": 0, "dead_lettered": 0}
    last_seen = time.monotonic()
    consumer.subscribe([EVENTS_TOPIC])
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                if max_idle_seconds and time.monotonic() - last_seen >= max_idle_seconds:
                    return counts
                continue
            if message.error():
                raise RuntimeError(f"Kafka error: {message.error()}")
            last_seen = time.monotonic()
            try:
                event = MetadataEvent.model_validate_json(message.value())
                mutation = transform_event(event)
            except (ValidationError, ValueError, TypeError, KeyError) as exc:
                _dlq(dlq_producer, message, exc)
                counts["dead_lettered"] += 1
            else:
                # EventConflict or Neo4j outage STOP the consumer (never silent DLQ).
                outcome = apply_event(driver, database, event, mutation)
                counts[outcome] += 1
                log.info("event_processed id=%s outcome=%s", event.event_id, outcome)
            # commit exact message offset + 1 after graph transaction or durable DLQ
            consumer.commit(message=message, asynchronous=False)
            if max_messages and sum(counts.values()) >= max_messages:
                return counts
    finally:
        consumer.close()
