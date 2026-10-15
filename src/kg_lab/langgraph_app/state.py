from typing import TypedDict


class AgentState(TypedDict, total=False):
    question: str
    route: str
    graph_context: list[dict]
    document_context: list[dict]
    answer: str
