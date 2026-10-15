from langgraph.graph import END, START, StateGraph

from kg_lab.langgraph_app.nodes import (
    document_retrieve,
    graph_retrieve,
    route_question,
    synthesize_answer,
)
from kg_lab.langgraph_app.state import AgentState


def _after_route(state: AgentState) -> str:
    return "documents" if state["route"] == "docs" else "graph"


def _after_graph(state: AgentState) -> str:
    return "documents" if state["route"] == "hybrid" else "synthesize"


def build_agent():
    workflow = StateGraph(AgentState)
    workflow.add_node("route", route_question)
    workflow.add_node("graph", graph_retrieve)
    workflow.add_node("documents", document_retrieve)
    workflow.add_node("synthesize", synthesize_answer)

    workflow.add_edge(START, "route")
    workflow.add_conditional_edges("route", _after_route, {"graph": "graph", "documents": "documents"})
    workflow.add_conditional_edges("graph", _after_graph, {"documents": "documents", "synthesize": "synthesize"})
    workflow.add_edge("documents", "synthesize")
    workflow.add_edge("synthesize", END)
    return workflow.compile()


agent = build_agent()
