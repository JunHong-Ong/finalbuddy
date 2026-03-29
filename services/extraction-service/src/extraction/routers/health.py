from fastapi import APIRouter

from extraction.keyword_processor import get_processor

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
async def health() -> dict:
    processor = get_processor()
    return {"status": "ok", "keywords_loaded": len(processor)}
