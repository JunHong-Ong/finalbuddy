from uuid import UUID

from bookbuddy_models.graph import DocumentNode
from fastapi import APIRouter, HTTPException
from neo4j.exceptions import Neo4jError
from pydantic import BaseModel

from graph.db import get_driver

router = APIRouter(prefix="/documents", tags=["documents"])


class ChunkPayload(BaseModel):
    id: UUID
    chunk_index: int
    text: str


class SegmentPayload(BaseModel):
    id: UUID
    index: int
    chunks: list[ChunkPayload]


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
async def create_segments(doc_id: UUID, segments: list[SegmentPayload]) -> None:
    driver = await get_driver()
    segments_data = [
        {
            "id": str(s.id),
            "index": s.index,
            "chunks": [
                {"id": str(c.id), "chunk_index": c.chunk_index, "text": c.text}
                for c in s.chunks
            ],
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
                      s.created_at = datetime(),
                      s.updated_at = datetime()
                  MERGE (d)-[:HAS_SEGMENT]->(s)
                  WITH s, seg
                  UNWIND seg.chunks AS ch
                    MERGE (c:Chunk {id: ch.id})
                    ON CREATE SET
                        c.chunk_index = ch.chunk_index,
                        c.text        = ch.text,
                        c.created_at  = datetime(),
                        c.updated_at  = datetime()
                    MERGE (s)-[:HAS_CHUNK]->(c)
                """,
                doc_id=str(doc_id),
                segments=segments_data,
            )
    except HTTPException:
        raise
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create segments: {e}")
