import re
from pathlib import Path

from kg_lab.hash_embedder import embed
from kg_lab.models import DocumentChunk


def _chunk(text: str, max_chars: int = 900) -> list[str]:
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    chunks: list[str] = []
    current = ""
    for paragraph in paragraphs:
        candidate = f"{current}\n\n{paragraph}".strip()
        if current and len(candidate) > max_chars:
            chunks.append(current)
            current = paragraph
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def extract_documents(docs_dir: Path, entity_names: dict[str, str], dimensions: int = 128) -> list[DocumentChunk]:
    chunks: list[DocumentChunk] = []
    for file in sorted(docs_dir.glob("*.md")):
        for idx, text in enumerate(_chunk(file.read_text(encoding="utf-8"))):
            lowered = text.lower()
            mentions = [entity_id for entity_id, name in entity_names.items() if name.lower() in lowered]
            chunks.append(
                DocumentChunk(
                    id=f"doc:{file.stem}:{idx}",
                    source=file.name,
                    text=text,
                    embedding=embed(text, dimensions),
                    mentions=mentions,
                )
            )
    return chunks
