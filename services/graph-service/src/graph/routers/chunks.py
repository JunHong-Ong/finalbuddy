from uuid import UUID

from bookbuddy_models import Chunk
from bookbuddy_models.extraction import ExtractionResult
from fastapi import APIRouter, HTTPException
from neo4j.exceptions import Neo4jError

from graph.db import get_driver

router = APIRouter(prefix="/chunks", tags=["chunks"])

_MENTION_LAMBDA = 2.0


@router.get("")
async def get_chunks(processed: bool | None = None) -> list[Chunk]:
    driver = await get_driver()
    try:
        async with driver.session() as session:
            if processed is None:
                result = await session.run(
                    """
                    MATCH (d:Document)-[:HAS_SEGMENT]->
                        (s:Segment)-[:HAS_CHUNK]->(c:Chunk)
                    RETURN c, s.uuid AS segment_id, d.uuid AS document_id
                    """
                )
            else:
                result = await session.run(
                    """
                    MATCH (d:Document)-[:HAS_SEGMENT]->
                        (s:Segment)-[:HAS_CHUNK]->(c:Chunk)
                    WHERE c.processed = $processed
                    RETURN c, s.uuid AS segment_id, d.uuid AS document_id
                    """,
                    processed=processed,
                )
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
                  SET r.weight = exp(-$lambda * c.chunk_position)
                """,
                {"chunk_id": str(chunk_id), "entities": entities, "lambda": _MENTION_LAMBDA},
            )
            await session.run(
                """
                WITH $entities AS entities
                UNWIND entities AS e1
                UNWIND entities AS e2
                WITH e1, e2 WHERE e1.keyword_id < e2.keyword_id
                MATCH (k1:Keyword {uuid: e1.keyword_id})
                MATCH (k2:Keyword {uuid: e2.keyword_id})
                MERGE (k1)-[r:CO_OCCURS_WITH]-(k2)
                ON CREATE SET r.count = 1
                ON MATCH SET r.count = r.count + 1
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
