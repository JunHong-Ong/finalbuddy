import httpx
from pydantic import BaseModel

from inference.config import settings


class SearchResult(BaseModel):
    chunk_id: str
    text: str
    score: float
    document_id: str
    segment_id: str


async def search_chunks(text: str, top_k: int = 5) -> list[SearchResult]:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{settings.graph_url}/search",
            json={"text": text, "top_k": top_k},
            timeout=30.0,
        )
        response.raise_for_status()
    return [SearchResult(**r) for r in response.json()]
