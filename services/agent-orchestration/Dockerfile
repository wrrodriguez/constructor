# services/agent-orchestration/Dockerfile
FROM python:3.12-slim AS base
WORKDIR /app
RUN pip install hatch

FROM base AS development
COPY pyproject.toml .
RUN pip install -e ".[dev]"
COPY . .

FROM base AS production
COPY pyproject.toml .
RUN pip install .
COPY src ./src
CMD ["python", "-m", "src.worker"]
