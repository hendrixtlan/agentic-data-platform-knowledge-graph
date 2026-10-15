from kg_lab.routing import classify_route


def test_hybrid_route_for_impact_and_why():
    assert classify_route("Why would downstream systems be affected?") == "hybrid"


def test_graph_route_for_lineage():
    assert classify_route("Show lineage and dependencies") == "graph"


def test_docs_route_for_architecture_decision():
    assert classify_route("Why was this architecture decision made?") == "docs"
