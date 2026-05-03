# ── Stage 1: Pull the latest Hermes Agent Python packages ─────────────
# We grab the installed site-packages from the official image so we
# automatically pick up any new Nous Research agent/LLM utilities on rebuild.
FROM nousresearch/hermes-agent:latest AS hermes-core

# ── Stage 2: Our runtime image (clean Python, no Docker deps) ─────────
FROM python:3.11-slim

ARG HERMES_VERSION=dev
LABEL hermes.version="${HERMES_VERSION}"
ENV HERMES_VERSION="${HERMES_VERSION}"

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy Hermes Agent core packages from the official image
# This gives us the latest Nous Research agent utilities without
# inheriting the Docker/Playwright/entrypoint baggage.
COPY --from=hermes-core /usr/local/lib/python3.*/dist-packages/ /usr/local/lib/python3.11/site-packages/

# Copy the current directory contents into the container at /app
COPY . /app

# Install HermesTrader-specific Python packages
RUN pip install --no-cache-dir \
    fastapi \
    "uvicorn[standard]" \
    watchfiles \
    sqlalchemy \
    "psycopg[binary]" \
    xgboost \
    pandas \
    numpy \
    requests \
    tenacity \
    ollama \
    "mcp[cli]"

# Set environment variables
ENV PYTHONPATH=/app
ENV HERMES_DSN="postgresql+psycopg://hermes:hermes@db:5432/hermes"
ENV HERMES_WATCHLIST="AAPL,SPY,QQQ,NVDA,AMD,KO"

# The command to run the application is defined in docker-compose.yml
