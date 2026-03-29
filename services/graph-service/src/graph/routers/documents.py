from uuid import UUID

from bookbuddy_models.graph import DocumentNode
from fastapi import APIRouter, HTTPException
from neo4j.exceptions import Neo4jError
from pydantic import BaseModel

from graph.db import get_driver

router = APIRouter(prefix="/documents", tags=["documents"])


class SegmentNodePayload(BaseModel):
    id: UUID
    index: int
    level: int
    next_segment_id: UUID | None
    parent_segment_id: UUID | None


class ChunkNodePayload(BaseModel):
    id: UUID
    chunk_index: int
    text: str
    segment_id: UUID
    next_chunk_id: UUID | None


class StatusUpdate(BaseModel):
    status: str


@router.post("", status_code=201)
async def create_document(document: DocumentNode) -> UUID:
    driver = await get_driver()
    data = document.model_dump(mode="json")
    try:
        async with driver.session() as session:
            await session.run(
                """
                MERGE (d:Document {content_hash: $content_hash})
                ON CREATE SET
                    d.id = $id,
                    d.file_type = $file_type,
                    d.status = $status,
                    d.created_at = datetime(),
                    d.updated_at = datetime()
                ON MATCH SET
                    d.status = $status,
                    d.updated_at = datetime()
                """,
                content_hash=data["content_hash"],
                id=str(data["id"]),
                file_type=data["file_type"],
                status=data["status"],
            )
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create document: {e}")
    return document.id


@router.get("")
async def get_documents(status: str | None = None) -> list[DocumentNode]:
    driver = await get_driver()
    try:
        async with driver.session() as session:
            if status:
                result = await session.run(
                    "MATCH (d:Document {status: $status}) RETURN d",
                    status=status,
                )
            else:
                result = await session.run("MATCH (d:Document) RETURN d")

            records = await result.data()
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch documents: {e}")

    return [
        DocumentNode(
            id=r["d"]["id"],
            content_hash=r["d"]["content_hash"],
            file_type=r["d"]["file_type"],
            status=r["d"]["status"],
        )
        for r in records
    ]


@router.get("/{doc_id}/status")
async def get_document_status(doc_id: UUID) -> str:
    driver = await get_driver()
    try:
        async with driver.session() as session:
            result = await session.run(
                "MATCH (d:Document {id: $id}) RETURN d.status AS status",
                id=str(doc_id),
            )
            record = await result.single()
    except Neo4jError as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to fetch document status: {e}"
        )

    if record is None:
        raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")

    return record["status"]


@router.patch("/{doc_id}", status_code=204)
async def update_document_status(doc_id: UUID, update: StatusUpdate) -> None:
    driver = await get_driver()
    try:
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (d:Document {id: $id})
                SET d.status = $status, d.updated_at = datetime()
                RETURN d
                """,
                id=str(doc_id),
                status=update.status,
            )
            record = await result.single()
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to update document: {e}")

    if record is None:
        raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")


@router.post("/{doc_id}/segments", status_code=201)
async def create_segments(doc_id: UUID, segments: list[SegmentNodePayload]) -> None:
    driver = await get_driver()
    segments_data = [
        {
            "id": str(s.id),
            "index": s.index,
            "level": s.level,
            "next_segment_id": str(s.next_segment_id) if s.next_segment_id else None,
            "parent_segment_id": str(s.parent_segment_id)
            if s.parent_segment_id
            else None,
        }
        for s in segments
    ]
    try:
        async with driver.session() as session:
            result = await session.run(
                "MATCH (d:Document {id: $doc_id}) RETURN d",
                doc_id=str(doc_id),
            )
            record = await result.single()
            if record is None:
                raise HTTPException(
                    status_code=404, detail=f"Document {doc_id} not found"
                )

            await session.run(
                """
                MATCH (d:Document {id: $doc_id})
                UNWIND $segments AS seg
                  MERGE (s:Segment {id: seg.id})
                  ON CREATE SET
                      s.index      = seg.index,
                      s.level      = seg.level,
                      s.created_at = datetime(),
                      s.updated_at = datetime()
                  MERGE (d)-[:HAS_SEGMENT]->(s)
                """,
                doc_id=str(doc_id),
                segments=segments_data,
            )
            await session.run(
                """
                UNWIND $segments AS seg
                WITH seg WHERE seg.next_segment_id IS NOT NULL
                MATCH (s1:Segment {id: seg.id})
                MATCH (s2:Segment {id: seg.next_segment_id})
                MERGE (s1)-[:NEXT]->(s2)
                """,
                segments=segments_data,
            )
            await session.run(
                """
                UNWIND $segments AS seg
                WITH seg WHERE seg.parent_segment_id IS NOT NULL
                MATCH (child:Segment  {id: seg.id})
                MATCH (parent:Segment {id: seg.parent_segment_id})
                MERGE (parent)-[:CONTAINS]->(child)
                """,
                segments=segments_data,
            )
    except HTTPException:
        raise
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create segments: {e}")


@router.post("/{doc_id}/chunks", status_code=201)
async def create_chunks(doc_id: UUID, chunks: list[ChunkNodePayload]) -> None:
    driver = await get_driver()
    chunks_data = [
        {
            "id": str(c.id),
            "chunk_index": c.chunk_index,
            "text": c.text,
            "segment_id": str(c.segment_id),
            "next_chunk_id": str(c.next_chunk_id) if c.next_chunk_id else None,
        }
        for c in chunks
    ]
    try:
        async with driver.session() as session:
            await session.run(
                """
                UNWIND $chunks AS ch
                  MERGE (c:Chunk {id: ch.id})
                  ON CREATE SET
                      c.chunk_index = ch.chunk_index,
                      c.text        = ch.text,
                      c.created_at  = datetime(),
                      c.updated_at  = datetime()
                  WITH c, ch
                  MATCH (s:Segment {id: ch.segment_id})
                  MERGE (s)-[:HAS_CHUNK]->(c)
                """,
                chunks=chunks_data,
            )
            await session.run(
                """
                UNWIND $chunks AS ch
                WITH ch WHERE ch.next_chunk_id IS NOT NULL
                MATCH (c1:Chunk {id: ch.id})
                MATCH (c2:Chunk {id: ch.next_chunk_id})
                MERGE (c1)-[:NEXT]->(c2)
                """,
                chunks=chunks_data,
            )
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create chunks: {e}")
