import hashlib
import math
import re


TOKEN_RE = re.compile(r"[A-Za-z0-9_\.:-]+")


def embed(text: str, dimensions: int = 128) -> list[float]:
    """Dependency-free hashing embedder for a fully local demo.

    This is intentionally simple: it makes the RAG path runnable without an external
    embedding API. Replace it with a production embedding model for real workloads.
    """
    vector = [0.0] * dimensions
    for token in TOKEN_RE.findall(text.lower()):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
        value = int.from_bytes(digest, "big")
        index = value % dimensions
        sign = -1.0 if (value >> 1) & 1 else 1.0
        vector[index] += sign
    norm = math.sqrt(sum(v * v for v in vector))
    return [v / norm for v in vector] if norm else vector
