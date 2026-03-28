from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from processing.config import settings
from processing.routers import chunks, health


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(title="processing-service", lifespan=lifespan)
app.include_router(health.router)
app.include_router(chunks.router)


def main():
    uvicorn.run(
        "processing.main:app",
        host=settings.processing_host,
        port=settings.processing_port,
        reload=True,
    )


if __name__ == "__main__":
    main()
