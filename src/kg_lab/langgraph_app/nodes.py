from neo4j import GraphDatabase

from kg_lab.config import settings
from kg_lab.langgraph_app.state import AgentState
from kg_lab.llm import synthesize
from kg_lab.retrieval import GraphRetriever, VectorRetriever
from kg_lab.streaming.impact import impact_analysis
from kg_lab.routing import classify_route


def route_question(state: AgentState) -> AgentState:
    return {"route": classify_route(state["question"])}


def graph_retrieve(state: AgentState) -> AgentState:
    driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        context = GraphRetriever(driver, settings.neo4j_database).retrieve(state["question"])
        # A deterministic impact-analysis tool complements general graph traversal.
        # No model-generated Cypher is executed against Neo4j.
        question = state["question"].lower()
        with driver.session(database=settings.neo4j_database) as session:
            subjects = [row["name"] for row in session.run(
                "MATCH (s:SchemaSubject) RETURN s.name AS name LIMIT 100")]
        for subject in subjects:
            if subject.lower() in question or subject.removesuffix("-value").lower() in question:
                context.insert(0, {
                    "type": "schema_impact",
                    "impact": impact_analysis(driver, settings.neo4j_database, subject),
                })
                break
    finally:
        driver.close()
    return {"graph_context": context}


def document_retrieve(state: AgentState) -> AgentState:
    driver = GraphDatabase.driver(settings.neo4j_uri, auth=(settings.neo4j_user, settings.neo4j_password))
    try:
        context = VectorRetriever(
            driver,
            settings.neo4j_database,
            settings.document_embedding_dimensions,
        ).retrieve(state["question"])
    finally:
        driver.close()
    return {"document_context": context}


def synthesize_answer(state: AgentState) -> AgentState:
    return {
        "answer": synthesize(
            state["question"],
            state.get("graph_context", []),
            state.get("document_context", []),
        )
    }
