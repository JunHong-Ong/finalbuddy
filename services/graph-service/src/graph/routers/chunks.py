from uuid import UUID

from bookbuddy_models import Chunk
from bookbuddy_models.extraction import ExtractionResult
from fastapi import APIRouter, HTTPException
from neo4j.exceptions import Neo4jError

from graph.db import get_driver

router = APIRouter(prefix="/chunks", tags=["chunks"])

_MENTION_LAMBDA = 2.0


@router.get("")
async def get_chunks(
    processed: bool | None = None,
    limit: int | None = 10,
) -> list[Chunk]:
    driver = await get_driver()

    base_query = """
        MATCH (d:Document)-[:HAS_SEGMENT]->
            (s:Segment)-[:HAS_CHUNK]->(c:Chunk)
        {where}
        RETURN c, s.uuid AS segment_id, d.uuid AS document_id
        {limit}
    """
    where_clause = "WHERE c.processed = $processed" if processed is not None else ""
    limit_clause = "LIMIT $limit" if limit is not None else ""
    query = base_query.format(where=where_clause, limit=limit_clause)
    params: dict = {}
    if processed is not None:
        params["processed"] = processed
    if limit is not None:
        params["limit"] = limit

    try:
        async with driver.session() as session:
            result = await session.run(query, params)
            records = await result.data()
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch chunks: {e}")

    return [
        Chunk(
            uuid=r["c"]["uuid"],
            document_id=r["document_id"],
            segment_id=r["segment_id"],
            chunk_index=r["c"]["chunk_index"],
            chunk_position=r["c"]["chunk_position"],
            text=r["c"]["text"],
        )
        for r in records
    ]


@router.post("/{chunk_id}/mentions", status_code=204)
async def create_mentions(chunk_id: UUID, result: ExtractionResult) -> None:
    driver = await get_driver()
    entities = [{"keyword_id": str(e.keyword_id)} for e in result.entities]
    try:
        async with driver.session() as session:
            chunk_record = await (
                await session.run(
                    "MATCH (c:Chunk {uuid: $uuid}) RETURN c",
                    uuid=str(chunk_id),
                )
            ).single()
            if chunk_record is None:
                raise HTTPException(
                    status_code=404, detail=f"Chunk {chunk_id} not found"
                )

            await session.run(
                """
                MATCH (c:Chunk {uuid: $chunk_id})
                UNWIND $entities AS e
                  MATCH (k:Keyword {uuid: e.keyword_id})
                  MERGE (k)-[r:MENTIONED_IN]->(c)
                  ON CREATE SET r.weight = exp(-$lambda * c.chunk_position),
                                k.mention_count = coalesce(k.mention_count, 0) + 1
                  ON MATCH SET  r.weight = exp(-$lambda * c.chunk_position)
                """,
                {
                    "chunk_id": str(chunk_id),
                    "entities": entities,
                    "lambda": _MENTION_LAMBDA,
                },
            )
            await session.run(
                """
                UNWIND $entities AS e
                WITH collect(DISTINCT e.keyword_id) AS keyword_ids
                UNWIND keyword_ids AS id1
                UNWIND keyword_ids AS id2
                WITH id1, id2 WHERE id1 < id2
                MATCH (k1:Keyword {uuid: id1})
                MATCH (k2:Keyword {uuid: id2})
                MERGE (k1)-[r:CO_OCCURS_WITH]-(k2)
                ON CREATE SET r.count = 1
                ON MATCH SET r.count = r.count + 1
                WITH k1, k2, r
                SET r.weight = toFloat(r.count) / (k1.mention_count + k2.mention_count - r.count)
                """,
                entities=entities,
            )
            await session.run(
                """
                MATCH (c:Chunk {uuid: $uuid})
                SET c.processed = true, c.updated_at = datetime()
                """,
                uuid=str(chunk_id),
            )
    except HTTPException:
        raise
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create mentions: {e}")
