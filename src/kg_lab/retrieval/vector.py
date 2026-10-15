from neo4j import Driver

from kg_lab.hash_embedder import embed


class VectorRetriever:
    def __init__(self, driver: Driver, database: str = "neo4j", dimensions: int = 128):
        self.driver = driver
        self.database = database
        self.dimensions = dimensions

    def retrieve(self, question: str, top_k: int = 4) -> list[dict]:
        vector = embed(question, self.dimensions)
        query = """
        CALL db.index.vector.queryNodes('document_embeddings', $top_k, $embedding)
        YIELD node, score
        OPTIONAL MATCH (node)-[:MENTIONS]->(entity)
        RETURN node.source AS source,
               node.text AS text,
               score,
               collect(DISTINCT coalesce(entity.name, entity.id)) AS mentions
        ORDER BY score DESC
        """
        with self.driver.session(database=self.database) as session:
            return [
                record.data()
                for record in session.run(query, top_k=top_k, embedding=vector)
            ]
