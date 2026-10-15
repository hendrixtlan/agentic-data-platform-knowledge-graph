from neo4j import Driver

from kg_lab.graph_schema import ALLOWED_LABELS, ALLOWED_RELATIONSHIPS, CONSTRAINTS
from kg_lab.models import DocumentChunk, Entity, Relationship


class GraphLoader:
    def __init__(self, driver: Driver, database: str = "neo4j", embedding_dimensions: int = 128):
        self.driver = driver
        self.database = database
        self.embedding_dimensions = embedding_dimensions

    def ensure_schema(self) -> None:
        with self.driver.session(database=self.database) as session:
            for statement in CONSTRAINTS:
                session.run(statement).consume()
            session.run(
                f"""
                CREATE VECTOR INDEX document_embeddings IF NOT EXISTS
                FOR (d:DocumentChunk) ON d.embedding
                OPTIONS {{indexConfig: {{
                  `vector.dimensions`: {self.embedding_dimensions},
                  `vector.similarity_function`: 'cosine'
                }}}}
                """
            ).consume()
            session.run("CALL db.awaitIndexes(60)").consume()

    def load_entities(self, entities: list[Entity]) -> None:
        grouped: dict[str, list[Entity]] = {}
        for entity in entities:
            if entity.label not in ALLOWED_LABELS:
                raise ValueError(f"Unsupported label: {entity.label}")
            grouped.setdefault(entity.label, []).append(entity)

        with self.driver.session(database=self.database) as session:
            for label, batch in grouped.items():
                rows = [
                    {"id": e.id, "properties": {"name": e.properties.get("name", e.id), **e.properties}}
                    for e in batch
                ]
                session.run(
                    f"""
                    UNWIND $rows AS row
                    MERGE (n:{label} {{id: row.id}})
                    SET n += row.properties,
                        n.updated_at = datetime()
                    """,
                    rows=rows,
                ).consume()

    def load_relationships(self, relationships: list[Relationship]) -> None:
        grouped: dict[str, list[Relationship]] = {}
        for rel in relationships:
            if rel.rel_type not in ALLOWED_RELATIONSHIPS:
                raise ValueError(f"Unsupported relationship: {rel.rel_type}")
            grouped.setdefault(rel.rel_type, []).append(rel)

        with self.driver.session(database=self.database) as session:
            for rel_type, batch in grouped.items():
                rows = [r.model_dump() for r in batch]
                session.run(
                    f"""
                    UNWIND $rows AS row
                    MATCH (a {{id: row.source_id}})
                    MATCH (b {{id: row.target_id}})
                    MERGE (a)-[r:{rel_type}]->(b)
                    SET r += row.properties,
                        r.updated_at = datetime()
                    """,
                    rows=rows,
                ).consume()

    def load_documents(self, chunks: list[DocumentChunk]) -> None:
        rows = [c.model_dump(exclude={"mentions"}) for c in chunks]
        mention_rows = [
            {"chunk_id": c.id, "entity_id": entity_id}
            for c in chunks
            for entity_id in c.mentions
        ]
        with self.driver.session(database=self.database) as session:
            session.run(
                """
                UNWIND $rows AS row
                MERGE (d:DocumentChunk {id: row.id})
                SET d.source = row.source,
                    d.text = row.text,
                    d.embedding = row.embedding,
                    d.updated_at = datetime()
                """,
                rows=rows,
            ).consume()
            if mention_rows:
                session.run(
                    """
                    UNWIND $rows AS row
                    MATCH (d:DocumentChunk {id: row.chunk_id})
                    MATCH (e {id: row.entity_id})
                    MERGE (d)-[:MENTIONS]->(e)
                    """,
                    rows=mention_rows,
                ).consume()
