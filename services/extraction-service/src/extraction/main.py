from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from extraction.client import fetch_keywords
from extraction.config import settings
from extraction.keyword_processor import init_processor
from extraction.routers import extract, health


@asynccontextmanager
async def lifespan(app: FastAPI):
    keywords = await fetch_keywords()
    init_processor(keywords)
    yield


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
