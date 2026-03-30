import asyncio
import logging
from contextlib import asynccontextmanager

import httpx
import uvicorn
from bookbuddy_models import Chunk
from fastapi import FastAPI

from extraction.client import fetch_keywords
from extraction.config import settings
from extraction.gliner import init_gliner
from extraction.keyword_processor import init_processor
from extraction.pipeline import run_pipeline
from extraction.routers import extract, health

logger = logging.getLogger(__name__)


async def poll_unprocessed_chunks(client: httpx.AsyncClient) -> None:
    while True:
        await asyncio.sleep(settings.poll_interval)
        try:
            response = await client.get("/chunks", params={"processed": "false"})
            response.raise_for_status()

            for chunk_data in response.json():
                chunk_id = chunk_data["uuid"]
                try:
                    chunk = Chunk(**chunk_data)
                    extraction_result = run_pipeline(chunk)

                    resp = await client.post(
                        f"/chunks/{chunk_id}/mentions",
                        content=extraction_result.model_dump_json(),
                        headers={"Content-Type": "application/json"},
                    )
                    resp.raise_for_status()

                    logger.info(
                        "Processed chunk %s: %d entities",
                        chunk_id,
                        len(extraction_result.entities),
                    )
                except Exception:
                    logger.exception("Failed to process chunk %s", chunk_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Error during chunk polling")


@asynccontextmanager
async def lifespan(app: FastAPI):
    keywords = await fetch_keywords()
    init_processor(keywords)
    init_gliner()

    async with httpx.AsyncClient(base_url=settings.graph_url) as client:
        poll_task = asyncio.create_task(poll_unprocessed_chunks(client))
        yield
        poll_task.cancel()
        await asyncio.gather(poll_task, return_exceptions=True)


app = FastAPI(title="extraction-service", lifespan=lifespan)
app.include_router(health.router)
app.include_router(extract.router)


def main():
    uvicorn.run(
        "extraction.main:app",
        host=settings.extraction_host,
        port=settings.extraction_port,
        reload=True,
    )


if __name__ == "__main__":
    main()
