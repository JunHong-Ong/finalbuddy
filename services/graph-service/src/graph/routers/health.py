from fastapi import APIRouter, HTTPException
from neo4j.exceptions import Neo4jError

from graph.db import get_driver

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
async def health() -> dict:
    driver = await get_driver()
    try:
        await driver.verify_connectivity()
    except Neo4jError as e:
        raise HTTPException(status_code=503, detail=f"Neo4j unreachable: {e}")
    return {"status": "ok", "neo4j": "connected"}
