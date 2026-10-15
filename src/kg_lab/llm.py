import json

from kg_lab.config import settings


def synthesize(question: str, graph_context: list[dict], document_context: list[dict]) -> str:
    if settings.openai_api_key:
        try:
            from langchain_openai import ChatOpenAI
        except ImportError as exc:
            raise RuntimeError("Install the LLM extra: pip install -e '.[llm]'") from exc

        model = ChatOpenAI(model=settings.openai_model, api_key=settings.openai_api_key)
        prompt = f"""You are a data-platform engineering assistant.
Answer only from the supplied evidence. Distinguish graph facts from documentary context.
If evidence is insufficient, say so.

QUESTION:
{question}

GRAPH EVIDENCE:
{graph_context}

DOCUMENT EVIDENCE:
{document_context}
"""
        return str(model.invoke(prompt).content)

    graph_lines = [
        (f"- Schema impact: {json.dumps(item['impact'], sort_keys=True)}"
         if item.get("type") == "schema_impact" else
         f"- {' -> '.join(item.get('nodes', []))} | rels={item.get('relationships', [])}")
        for item in graph_context[:8]
    ]
    doc_lines = [
        f"- {item.get('source')}: {item.get('text', '')[:260].replace(chr(10), ' ')}"
        for item in document_context[:4]
    ]
    parts = [f"Question: {question}"]
    if graph_lines:
        parts.append("Graph evidence:\n" + "\n".join(graph_lines))
    if doc_lines:
        parts.append("Document evidence:\n" + "\n".join(doc_lines))
    if not graph_lines and not doc_lines:
        parts.append("No supporting evidence was retrieved.")
    parts.append("Local mode is extractive; set OPENAI_API_KEY for generative synthesis.")
    return "\n\n".join(parts)
