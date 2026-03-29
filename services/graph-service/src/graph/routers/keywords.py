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
                MATCH (k:Keyword)<-[:SYNONYM_FOR]-(a:Alias)
                RETURN
                    k.display_name AS name,
                    a.display_name AS aliases
                """
            )
            records = await result.data()
    except Neo4jError as e:
        raise HTTPException(status_code=500, detail=f"Failed to fetch keywords: {e}")
    return [Keyword(name=r["name"], aliases=r["aliases"] or []) for r in records]


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
