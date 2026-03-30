FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY shared/ shared/
COPY services/ingestion-service/pyproject.toml services/ingestion-service/pyproject.toml

RUN uv sync --package ingestion-service --frozen

CMD ["uv", "run", "--package", "ingestion-service", "ingestion-service"]
