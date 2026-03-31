import httpx
from pydantic import BaseModel

from inference.config import settings


class SearchResult(BaseModel):
    chunk_id: str
    text: str
    score: float
    document_id: str
    segment_id: str


class ChunkInfo(BaseModel):
    uuid: str
    text: str


class KeywordWithChunks(BaseModel):
    uuid: str
    display_name: str
    chunks: list[ChunkInfo]


class StudyGraph(BaseModel):
    keywords: list[dict]
    edges: list[dict]
    keyword_chunks: list[KeywordWithChunks]


async def fetch_study_graph(uuids: list[str], top_k: int) -> StudyGraph:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{settings.graph_url}/keywords/study-graph",
            json={"uuids": uuids, "top_k": top_k},
            timeout=60.0,
        )
        response.raise_for_status()
    return StudyGraph(**response.json())


async def search_chunks(text: str, top_k: int = 5) -> list[SearchResult]:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{settings.graph_url}/search",
            json={"text": text, "top_k": top_k},
            timeout=30.0,
        )
        response.raise_for_status()
    return [SearchResult(**r) for r in response.json()]
