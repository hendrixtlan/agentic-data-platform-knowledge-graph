import sys
import types

import pytest

from kg_lab.streaming import broker
from kg_lab.streaming.events import make_event


class Message:
    def __init__(self, body):
        self.body = body

    def value(self): return self.body
    def key(self): return b"k"
    def error(self): return None
    def topic(self): return broker.EVENTS_TOPIC
    def partition(self): return 0
    def offset(self): return 4


class DummyConsumer:
    latest = None

    def __init__(self, _config):
        self.items = list(DummyConsumer.latest)
        self.committed = []
        DummyConsumer.instance = self

    def subscribe(self, topics): pass
    def poll(self, seconds): return self.items.pop(0) if self.items else None
    def commit(self, message, asynchronous): self.committed.append(message)
    def close(self): pass


class DummyProducer:
    instance = None
    def __init__(self, _config):
        self.messages = []
        DummyProducer.instance = self

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.messages.append((topic, value))
        if on_delivery: on_delivery(None, None)

    def flush(self, timeout): return 0


def _mock_kafka(monkeypatch):
    monkeypatch.setitem(sys.modules, "confluent_kafka",
                        types.SimpleNamespace(Consumer=DummyConsumer, Producer=DummyProducer))
    monkeypatch.setattr(broker, "ensure_topics", lambda bootstrap: None)


def test_bad_message_dlq_then_commit(monkeypatch):
    _mock_kafka(monkeypatch)
    DummyConsumer.latest = [Message(b"not-json")]
    outcome = broker.consume_forever(None, "neo4j", "fake", max_messages=1)
    assert outcome["dead_lettered"] == 1
    assert len(DummyConsumer.instance.committed) == 1
    assert DummyProducer.instance.messages[0][0] == broker.DLQ_TOPIC


def test_db_failure_preserves_uncommitted_offset(monkeypatch):
    _mock_kafka(monkeypatch)
    event = make_event("dbt_manifest_snapshot", "dbt_artifact", {"manifest": {"models": []}}, "x")
    DummyConsumer.latest = [Message(event.to_json())]
    monkeypatch.setattr(broker, "apply_event", lambda *args: (_ for _ in ()).throw(RuntimeError("db down")))
    with pytest.raises(RuntimeError, match="db down"):
        broker.consume_forever(None, "neo4j", "fake", max_messages=1)
    assert not DummyConsumer.instance.committed
    assert not DummyProducer.instance.messages


def test_success_commits_after_projection(monkeypatch):
    _mock_kafka(monkeypatch)
    event = make_event("dbt_manifest_snapshot", "dbt_artifact", {"manifest": {"models": []}}, "x")
    DummyConsumer.latest = [Message(event.to_json())]
    monkeypatch.setattr(broker, "apply_event", lambda *args: "applied")
    outcome = broker.consume_forever(None, "neo4j", "fake", max_messages=1)
    assert outcome == {"applied": 1, "duplicate": 0, "dead_lettered": 0}
    assert len(DummyConsumer.instance.committed) == 1
