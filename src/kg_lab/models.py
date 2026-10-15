from typing import Any

from pydantic import BaseModel, Field


class Entity(BaseModel):
    id: str
    label: str
    properties: dict[str, Any] = Field(default_factory=dict)


class Relationship(BaseModel):
    source_id: str
    target_id: str
    rel_type: str
    properties: dict[str, Any] = Field(default_factory=dict)


class DocumentChunk(BaseModel):
    id: str
    source: str
    text: str
    embedding: list[float]
    mentions: list[str] = Field(default_factory=list)


class ExtractionBatch(BaseModel):
    entities: list[Entity] = Field(default_factory=list)
    relationships: list[Relationship] = Field(default_factory=list)
    documents: list[DocumentChunk] = Field(default_factory=list)
