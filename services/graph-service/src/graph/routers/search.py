from fastapi import APIRouter, HTTPException
from neo4j.exceptions import Neo4jError
from pydantic import BaseModel

from graph.db import get_driver
from graph.embedder import embed

router = APIRouter(prefix="/search", tags=["search"])


class SearchRequest(BaseModel):
    text: str
    top_k: int = 5


class SearchResult(BaseModel):
    chunk_id: str
    text: str
    score: float
    document_id: str
    segment_id: str


@router.post("", response_model=list[SearchResult])
async def search_chunks(request: SearchRequest) -> list[SearchResult]:
    embedding = embed(request.text)
    driver = await get_driver()
    try:
        async with driver.session() as session:
            result = await session.run(
                """
                CALL db.index.vector.queryNodes('embeddings_Chunk', $top_k, $embedding)
                YIELD node AS c, score
                MATCH (d:Document)-[:HAS_SEGMENT]->(s:Segment)-[:HAS_CHUNK]->(c)
                RETURN c.uuid AS chunk_id, c.text AS text, score,
                       d.uuid AS document_id, s.uuid AS segment_id
                """,
                top_k=request.top_k,
                embedding=embedding,
            )
            records = await result.data()
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {e}")

    return [
        SearchResult(
            chunk_id=r["chunk_id"],
            text=r["text"],
            score=r["score"],
            document_id=r["document_id"],
            segment_id=r["segment_id"],
        )
        for r in records
    ]
