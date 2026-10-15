GRAPH_TERMS = {
    "depend",
    "depends",
    "dependency",
    "dependencies",
    "downstream",
    "upstream",
    "lineage",
    "impact",
    "owner",
    "owned",
    "break",
    "affect",
    "affected",
    "schema",
    "schemas",
    "change",
    "changes",
    "breaking",
    "version",
    "consumes",
    "produces",
}
DOC_TERMS = {"why", "reason", "decision", "adr", "documentation", "document", "policy"}


def classify_route(question: str) -> str:
    words = set(question.lower().replace("?", "").split())
    has_graph = bool(words & GRAPH_TERMS)
    has_docs = bool(words & DOC_TERMS)
    if has_graph and has_docs:
        return "hybrid"
    if has_graph:
        return "graph"
    if has_docs:
        return "docs"
    return "hybrid"
