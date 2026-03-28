from fastapi import APIRouter, HTTPException

from inference.client import get_client

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
async def health() -> dict:
    client = await get_client()
    try:
        await client.models.list()
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"LLM backend unreachable: {e}")
    return {"status": "ok", "llm": "connected"}
