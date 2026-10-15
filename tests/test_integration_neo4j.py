import os

import pytest

pytestmark = pytest.mark.integration


@pytest.mark.skipif(os.getenv("RUN_INTEGRATION") != "1", reason="set RUN_INTEGRATION=1")
def test_seed_and_hybrid_retrieval_against_neo4j():
    from neo4j import GraphDatabase

    from kg_lab.config import settings
    from kg_lab.pipeline import seed_graph
    from kg_lab.retrieval import GraphRetriever, VectorRetriever

    counts = seed_graph()
    assert counts["entities"] > 0
    assert counts["relationships"] > 0
    assert counts["document_chunks"] > 0

    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    try:
        graph_hits = GraphRetriever(driver, settings.neo4j_database).retrieve(
            "What depends on customer_events?"
        )
        vector_hits = VectorRetriever(
            driver,
            settings.neo4j_database,
            settings.document_embedding_dimensions,
        ).retrieve("Why was Kafka selected for customer_events?")
    finally:
        driver.close()

    assert graph_hits
    assert vector_hits
    assert any("customer_events" in hit["text"] for hit in vector_hits)
