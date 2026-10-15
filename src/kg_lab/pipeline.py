from neo4j import GraphDatabase

from kg_lab.config import DATA_DIR, settings
from kg_lab.extractors import extract_all_structured, extract_documents
from kg_lab.graph_loader import GraphLoader


def seed_graph() -> dict[str, int]:
    structured = extract_all_structured(DATA_DIR)
    names = {entity.id: entity.properties.get("name", entity.id) for entity in structured.entities}
    docs = extract_documents(
        DATA_DIR / "docs",
        entity_names=names,
        dimensions=settings.document_embedding_dimensions,
    )

    driver = GraphDatabase.driver(
        settings.neo4j_uri,
        auth=(settings.neo4j_user, settings.neo4j_password),
    )
    try:
        loader = GraphLoader(
            driver,
            database=settings.neo4j_database,
            embedding_dimensions=settings.document_embedding_dimensions,
        )
        loader.ensure_schema()
        loader.load_entities(structured.entities)
        loader.load_relationships(structured.relationships)
        loader.load_documents(docs)
    finally:
        driver.close()

    return {
        "entities": len(structured.entities),
        "relationships": len(structured.relationships),
        "document_chunks": len(docs),
    }
