"""Validated envelopes; messages carry JSON metadata, not Avro business records."""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

EventType = Literal[
    "schema_version_observed",
    "schema_change_proposed",
    "openlineage_run_event",
    "dbt_manifest_snapshot",
]


class MetadataEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str = Field(min_length=1)
    event_type: EventType
    source: str = Field(min_length=1)
    occurred_at: datetime
    payload: dict[str, Any]

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone")
        return value

    def fingerprint(self) -> str:
        """Detect re-use of an event_id with different content."""
        body = self.model_dump(mode="json", exclude={"event_id"})
        encoded = json.dumps(body, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def to_json(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


def make_event(
    event_type: EventType,
    source: str,
    payload: dict[str, Any],
    natural_key: str,
    occurred_at: datetime | None = None,
) -> MetadataEvent:
    """Stable ID for the same content. Timestamp is fixed once created by the publisher."""
    occurred_at = occurred_at or datetime.now(timezone.utc)
    payload_hash = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:24]
    # Observing the same unchanged metadata twice is a NEW observation; only
    # replay of the exact original envelope is the same event.
    stamp = occurred_at.isoformat()
    event_id = f"{source}:{event_type}:{natural_key}:{payload_hash}:{stamp}"
    return MetadataEvent(
        event_id=event_id,
        event_type=event_type,
        source=source,
        occurred_at=occurred_at,
        payload=payload,
    )


def partition_key(event: MetadataEvent) -> str:
    """Messages concerning one schema subject share a Kafka partition."""
    p = event.payload
    if event.event_type.startswith("schema_"):
        return f"subject:{p['subject']}"
    if event.event_type == "openlineage_run_event":
        job = p["job"]
        return f"job:{job['namespace']}:{job['name']}"
    return "dbt:manifest"
