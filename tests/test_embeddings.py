from kg_lab.hash_embedder import embed


def test_embedding_has_expected_dimensions_and_unit_norm():
    vector = embed("customer_events Kafka Customer360", dimensions=64)
    assert len(vector) == 64
    norm = sum(v * v for v in vector) ** 0.5
    assert abs(norm - 1.0) < 1e-9


def test_same_text_is_deterministic():
    assert embed("same text", 32) == embed("same text", 32)
