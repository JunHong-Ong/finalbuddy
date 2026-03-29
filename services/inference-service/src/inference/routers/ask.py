from fastapi import APIRouter, HTTPException
from openai import OpenAIError
from pydantic import BaseModel

from inference.client import get_client
from inference.config import GENERATION_MODEL
from inference.graph_client import SearchResult, search_chunks
from inference.prompts import load_system_prompt

router = APIRouter(prefix="/ask", tags=["ask"])

_system_prompt = load_system_prompt("ask")


class AskRequest(BaseModel):
    question: str
    top_k: int = 5


class AskResponse(BaseModel):
    answer: str
    sources: list[SearchResult]


@router.post("", response_model=AskResponse)
async def ask(request: AskRequest) -> AskResponse:
    sources = await search_chunks(request.question, top_k=request.top_k)

    context_lines = [
        f"[{i + 1}] {r.text}" for i, r in enumerate(sources)
    ]
    context = "\n\n".join(context_lines)
    user_input = f"Sources:\n{context}\n\nQuestion: {request.question}"

    client = await get_client()
    try:
        response = await client.responses.create(
            model=GENERATION_MODEL,
            instructions=_system_prompt,
            input=user_input,
        )
    except OpenAIError as e:
        raise HTTPException(status_code=502, detail=f"Generation failed: {e}")

    return AskResponse(answer=response.output_text, sources=sources)
