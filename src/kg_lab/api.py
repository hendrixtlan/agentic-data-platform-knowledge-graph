from fastapi import FastAPI
from pydantic import BaseModel

from kg_lab.langgraph_app import agent

app = FastAPI(title="Agentic Data Platform Knowledge Graph", version="0.2.0")


class AskRequest(BaseModel):
    question: str


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/ask")
def ask(request: AskRequest):
    result = agent.invoke({"question": request.question})
    return {
        "route": result.get("route"),
        "answer": result.get("answer"),
        "graph_context": result.get("graph_context", []),
        "document_context": result.get("document_context", []),
    }


@app.get("/impact/{subject}")
def get_impact(subject: str):
    from kg_lab.streaming.service import get_impact as analyze

    result = analyze(subject)
    if not result["found"]:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail="Subject not found")
    return result
