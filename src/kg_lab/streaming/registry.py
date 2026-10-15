"""Confluent-compatible REST Schema Registry adapter (Redpanda or Confluent)."""
from __future__ import annotations

import json
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


class RegistryError(RuntimeError):
    pass


class SchemaRegistryClient:
    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, payload: dict | None = None):
        data = json.dumps(payload).encode() if payload is not None else None
        request = Request(
            self.base_url + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json", "Accept": "application/vnd.schemaregistry.v1+json"},
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                raw = response.read().decode()
                return json.loads(raw) if raw else {}
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:600]
            raise RegistryError(f"Schema Registry {exc.code}: {detail}") from exc

    def register(self, subject: str, schema: dict) -> dict:
        segment = quote(subject, safe="")
        return self._request("POST", f"/subjects/{segment}/versions", {"schemaType": "AVRO", "schema": json.dumps(schema)})

    def version(self, subject: str, version: str = "latest") -> dict:
        segment = quote(subject, safe="")
        return self._request("GET", f"/subjects/{segment}/versions/{version}")

    def compatibility(self, subject: str) -> str:
        segment = quote(subject, safe="")
        return self._request("GET", f"/config/{segment}?defaultToGlobal=true").get("compatibilityLevel", "UNKNOWN")

    def test_compatibility(self, subject: str, schema: dict, version: str = "latest") -> bool:
        segment = quote(subject, safe="")
        answer = self._request("POST", f"/compatibility/subjects/{segment}/versions/{version}", {"schemaType": "AVRO", "schema": json.dumps(schema)})
        return bool(answer["is_compatible"])
