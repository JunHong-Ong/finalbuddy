from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from inference.client import close_client, get_client
from inference.config import settings
from inference.routers import ask, generation, health, plan, synth


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_client()
    yield
    await close_client()


app = FastAPI(title="inference-service", lifespan=lifespan)
app.include_router(health.router)
app.include_router(generation.router)
app.include_router(ask.router)
app.include_router(plan.router)
app.include_router(synth.router)


def main():
    uvicorn.run(
        "inference.main:app",
        host=settings.inference_host,
        port=settings.inference_port,
        reload=True,
    )


if __name__ == "__main__":
    main()
