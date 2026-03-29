from collections import defaultdict

from fastapi import APIRouter, HTTPException
from openai import OpenAIError
from pydantic import BaseModel

from inference.client import get_client
from inference.config import GENERATION_MODEL
from inference.graph_client import SearchResult, search_chunks
from inference.prompts import load_system_prompt

router = APIRouter(prefix="/synth", tags=["synth"])

_system_prompt = load_system_prompt("synth")


class SynthRequest(BaseModel):
    question: str
    top_k: int = 10


class SynthResponse(BaseModel):
    synthesis: str
    document_ids: list[str]
    sources: list[SearchResult]


@router.post("", response_model=SynthResponse)
async def synthesize(request: SynthRequest) -> SynthResponse:
    sources = await search_chunks(request.question, top_k=request.top_k)

    by_doc: dict[str, list[SearchResult]] = defaultdict(list)
    for r in sources:
        by_doc[r.document_id].append(r)

    context_sections = []
    for doc_idx, (doc_id, chunks) in enumerate(by_doc.items(), 1):
        passages = "\n\n".join(f"Passage {j + 1}: {c.text}" for j, c in enumerate(chunks))
        context_sections.append(f"[Doc {doc_idx} | id: {doc_id}]\n{passages}")
    context = "\n\n---\n\n".join(context_sections)

    user_input = f"Question: {request.question}\n\nDocuments:\n{context}"

    client = await get_client()
    try:
        response = await client.responses.create(
            model=GENERATION_MODEL,
            instructions=_system_prompt,
            input=user_input,
        )
    except OpenAIError as e:
        raise HTTPException(status_code=502, detail=f"Generation failed: {e}")

    return SynthResponse(
        synthesis=response.output_text,
        document_ids=list(by_doc.keys()),
        sources=sources,
    )
