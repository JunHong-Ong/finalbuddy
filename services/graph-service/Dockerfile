FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY openalex/csv/ openalex/csv/
COPY scripts/initialize_database.py scripts/initialize_database.py

COPY pyproject.toml uv.lock ./
COPY shared/ shared/
COPY services/graph-service/pyproject.toml services/graph-service/pyproject.toml

RUN uv sync --package graph-service --frozen

CMD ["uv", "run", "--package", "graph-service", "graph-service"]
