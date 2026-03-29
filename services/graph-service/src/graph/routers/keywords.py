from bookbuddy_models import Keyword
from fastapi import APIRouter, HTTPException
from neo4j.exceptions import Neo4jError

from graph.db import get_driver

router = APIRouter(prefix="/keywords", tags=["keywords"])


@router.get("")
async def list_keywords() -> list[Keyword]:
    driver = await get_driver()
    try:
        async with driver.session() as session:
            result = await session.run(
                """
                MATCH (k:Keyword)
                RETURN
                    k.uuid AS uuid,
                    k.created_at AS created_at,
                    k.updated_at AS updated_at,
                    k.display_name AS display_name,
                    k.openalex_id AS openalex_id,
                    k.cited_by_count AS cited_by_count,
                    k.works_count AS works_count
                """
            )
            records = await result.data()
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch keywords: {e}")
    return [
        Keyword(
            uuid=r["uuid"],
            created_at=r["created_at"].to_native(),
            updated_at=r["updated_at"].to_native(),
            openalex_id=r["openalex_id"],
            display_name=r["display_name"],
            works_count=r["works_count"],
            cited_by_count=r["cited_by_count"],
        )
        for r in records
    ]


@router.post("", status_code=201)
async def create_keyword(keyword: Keyword) -> Keyword:
    driver = await get_driver()
    try:
        async with driver.session() as session:
            await session.run(
                """
                MERGE (k:Keyword {name: $name})
                ON CREATE SET k.aliases = $aliases
                ON MATCH SET k.aliases = $aliases
                """,
                name=keyword.name,
                aliases=keyword.aliases,
            )
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to create keyword: {e}")
    return keyword
