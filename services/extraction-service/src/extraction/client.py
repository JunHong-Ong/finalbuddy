import httpx
from bookbuddy_models import Keyword

from extraction.config import settings


async def fetch_keywords() -> list[Keyword]:
    async with httpx.AsyncClient() as client:
        response = await client.get(f"{settings.graph_url}/keywords")
        response.raise_for_status()
        return [Keyword(**kw) for kw in response.json()]
