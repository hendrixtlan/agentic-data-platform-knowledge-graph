from neo4j import Driver


class GraphRetriever:
    def __init__(self, driver: Driver, database: str = "neo4j"):
        self.driver = driver
        self.database = database

    def retrieve(self, question: str, limit: int = 20) -> list[dict]:
        query = """
        MATCH (anchor)
        WHERE anchor.name IS NOT NULL
          AND toLower($question) CONTAINS toLower(anchor.name)
        WITH anchor ORDER BY size(anchor.name) DESC LIMIT 1
        OPTIONAL MATCH p=(anchor)-[*1..5]-(related)
        WITH anchor, p, related
        WHERE p IS NOT NULL
        RETURN anchor.name AS anchor,
               [n IN nodes(p) | coalesce(n.name, n.id)] AS nodes,
               [r IN relationships(p) | type(r)] AS relationships
        LIMIT $limit
        """
        with self.driver.session(database=self.database) as session:
            return [record.data() for record in session.run(query, question=question, limit=limit)]
