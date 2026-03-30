FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY shared/ shared/
COPY services/inference-service/pyproject.toml services/inference-service/pyproject.toml

RUN uv sync --package inference-service --frozen

CMD ["uv", "run", "--package", "inference-service", "inference-service"]
