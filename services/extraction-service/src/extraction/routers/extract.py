from bookbuddy_models import Chunk, ExtractionResult
from fastapi import APIRouter

from extraction.pipeline import run_pipeline

router = APIRouter(prefix="/extract", tags=["extract"])


@router.post("")
async def extract(chunk: Chunk) -> ExtractionResult:
    return run_pipeline(chunk)
