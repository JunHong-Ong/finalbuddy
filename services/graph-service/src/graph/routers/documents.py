from uuid import UUID

from bookbuddy_models import Document
from fastapi import APIRouter, HTTPException
from neo4j.exceptions import Neo4jError

from graph.db import get_driver

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post("", status_code=201)
async def create_document(document: Document) -> UUID:
    driver = await get_driver()
    data = document.model_dump(mode="json")
    try:
        async with driver.session() as session:
            await session.run(
                """
                MERGE (d:Document {id: $id})
                SET d += $props
                """,
                id=data["id"],
                props=data,
            )
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create document: {e}")
    return document.id
