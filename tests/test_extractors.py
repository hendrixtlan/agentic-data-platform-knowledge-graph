from kg_lab.config import DATA_DIR
from kg_lab.extractors.structured import extract_all_structured


def test_structured_extraction_builds_expected_entities_and_relationships():
    batch = extract_all_structured(DATA_DIR)
    ids = {e.id for e in batch.entities}
    rels = {(r.source_id, r.rel_type, r.target_id) for r in batch.relationships}

    assert "topic:customer_events" in ids
    assert "dataset:gold.customer_360" in ids
    assert "dashboard:Customer Executive Dashboard" in ids
    assert (
        "pipeline:CustomerStreaming",
        "CONSUMES",
        "topic:customer_events",
    ) in rels
    assert (
        "dataset:gold.customer_360",
        "CONSUMED_BY",
        "dashboard:Customer Executive Dashboard",
    ) in rels
