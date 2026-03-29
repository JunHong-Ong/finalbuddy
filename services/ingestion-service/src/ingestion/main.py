import asyncio
import hashlib
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import uvicorn
from bookbuddy_models.graph import DocumentNode
from fastapi import FastAPI, File, HTTPException, UploadFile
from pydantic import BaseModel

from .chunkers.recursive import RecursiveChunker
from .config import settings
from .factory import ParserFactory
from .graph import build_segment_payloads

logger = logging.getLogger(__name__)


async def poll_queued_documents(
    client: httpx.AsyncClient, factory: ParserFactory, chunker: RecursiveChunker
) -> None:
    while True:
        await asyncio.sleep(settings.poll_interval)
        try:
            response = await client.get("/documents", params={"status": "QUEUED"})
            response.raise_for_status()

            for node_data in response.json():
                doc_id = UUID(node_data["id"])
                file_type = node_data["file_type"]

                staged_path = Path(settings.staging_dir) / f"{doc_id}.{file_type}"
                if not staged_path.exists():
                    logger.warning("Staged file not found for document %s", doc_id)
                    continue

                await client.patch(
                    f"/documents/{doc_id}",
                    json={"status": "PROCESSING"},
                )

                try:
                    document = factory.parse(staged_path.read_bytes(), file_type)
                    staged_path.unlink()

                    chunks = chunker.chunk(document)

                    payloads = build_segment_payloads(document, chunks)
                    resp = await client.post(
                        f"/documents/{doc_id}/segments", json=payloads
                    )
                    resp.raise_for_status()

                    await client.patch(
                        f"/documents/{doc_id}",
                        json={"status": "SUCCEEDED"},
                    )
                    logger.info("Processed document %s: %d chunks", doc_id, len(chunks))
                except Exception:
                    logger.exception("Failed to process document %s", doc_id)
                    await client.patch(
                        f"/documents/{doc_id}",
                        json={"status": "FAILED"},
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error during document polling")


@asynccontextmanager
async def lifespan(app: FastAPI):
    Path(settings.staging_dir).mkdir(parents=True, exist_ok=True)
    factory = ParserFactory()
    chunker = RecursiveChunker()

    async with httpx.AsyncClient(base_url=settings.graph_service_url) as client:
        app.state.http_client = client
        poll_task = asyncio.create_task(poll_queued_documents(client, factory, chunker))
        yield
        poll_task.cancel()
        await asyncio.gather(poll_task, return_exceptions=True)


app = FastAPI(title="Ingestion Service", lifespan=lifespan)


class IngestResponse(BaseModel):
    id: UUID


@app.post("/ingest", response_model=IngestResponse)
async def ingest(file: UploadFile = File(...)) -> IngestResponse:
    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    if not ext:
        raise HTTPException(status_code=415, detail="Could not determine file type")

    data = await file.read()
    content_hash = hashlib.sha256(data).hexdigest()

    document_node = DocumentNode(
        id=uuid4(),
        content_hash=content_hash,
        file_type=ext,
        status="QUEUED",
    )

    response = await app.state.http_client.post(
        "/documents",
        content=document_node.model_dump_json(),
        headers={"Content-Type": "application/json"},
    )
    response.raise_for_status()

    staged_path = Path(settings.staging_dir) / f"{document_node.id}.{ext}"
    staged_path.write_bytes(data)

    return IngestResponse(id=document_node.id)


def main():
    uvicorn.run(
        "ingestion.main:app",
        host=settings.ingestion_host,
        port=settings.ingestion_port,
        reload=True,
    )


if __name__ == "__main__":
    main()
