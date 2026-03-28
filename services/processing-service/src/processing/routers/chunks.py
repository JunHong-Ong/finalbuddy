from bookbuddy_models import Chunk, Document
from fastapi import APIRouter

from processing.chunkers.recursive import RecursiveChunker

router = APIRouter(prefix="/chunks", tags=["chunks"])

_chunker = RecursiveChunker()


@router.post("")
async def create_chunks(document: Document) -> list[Chunk]:
    return _chunker.chunk(document)
