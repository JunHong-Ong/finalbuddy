from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from graph.config import settings
from graph.db import close_driver, get_driver
from graph.routers import health


@asynccontextmanager
async def lifespan(app: FastAPI):
    await get_driver()
    yield
    await close_driver()


app = FastAPI(title="graph-service", lifespan=lifespan)
app.include_router(health.router)


def main():
    uvicorn.run(
        "graph.main:app",
        host=settings.graph_host,
        port=settings.graph_port,
        reload=True,
    )


if __name__ == "__main__":
    main()
