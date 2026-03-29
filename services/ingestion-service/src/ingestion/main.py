from contextlib import asynccontextmanager

import uvicorn
from bookbuddy_models import Document
from fastapi import FastAPI, File, HTTPException, UploadFile

from .config import settings
from .factory import ParserFactory


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.factory = ParserFactory()
    yield


app = FastAPI(title="Ingestion Service", lifespan=lifespan)


@app.post("/ingest", response_model=Document)
async def ingest(file: UploadFile = File(...)) -> Document:
    ext = (file.filename or "").rsplit(".", 1)[-1].lower()
    data = await file.read()

    try:
        return app.state.factory.parse(data, ext)
    except ValueError as exc:
        raise HTTPException(status_code=415, detail=str(exc)) from exc


def main():
    uvicorn.run(
        "ingestion.main:app",
        host=settings.ingestion_host,
        port=settings.ingestion_port,
        reload=True,
    )


if __name__ == "__main__":
    main()
