from fastapi import APIRouter, HTTPException
from openai import OpenAIError
from pydantic import BaseModel

from inference.client import get_client
from inference.config import EMBEDDING_MODEL

router = APIRouter(prefix="/embeddings", tags=["embeddings"])


class EmbeddingRequest(BaseModel):
    input: str


@router.post("")
async def create_embedding(request: EmbeddingRequest):
    client = await get_client()
    try:
        response = await client.embeddings.create(
            model=EMBEDDING_MODEL,
            input=request.input,
        )
    except OpenAIError as e:
        raise HTTPException(status_code=502, detail=f"Embedding failed: {e}")
    return response
