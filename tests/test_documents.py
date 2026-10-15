from kg_lab.config import DATA_DIR
from kg_lab.extractors.documents import extract_documents


def test_document_extraction_links_known_entities():
    known = {
        "topic:customer_events": "customer_events",
        "pipeline:CustomerStreaming": "CustomerStreaming",
    }
    chunks = extract_documents(DATA_DIR / "docs", known, dimensions=32)
    assert chunks
    assert any("topic:customer_events" in chunk.mentions for chunk in chunks)
