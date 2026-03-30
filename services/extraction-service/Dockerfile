FROM python:3.12-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY shared/ shared/
COPY services/extraction-service/pyproject.toml services/extraction-service/pyproject.toml
COPY services/inference-service/pyproject.toml services/inference-service/pyproject.toml
COPY services/inference-service/src/ services/inference-service/src/

RUN uv sync --package extraction-service --frozen

CMD ["uv", "run", "--package", "extraction-service", "extraction-service"]
