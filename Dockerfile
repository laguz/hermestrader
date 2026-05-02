# ── Stage 1: Official Hermes Agent core from Nous Research ────────────
# Pulls the latest agent platform (Python, tools, runtime). Rebuilding
# this image automatically picks up any upstream core updates.
FROM nousresearch/hermes-agent:latest

ARG HERMES_VERSION=dev
LABEL hermes.version="${HERMES_VERSION}"
ENV HERMES_VERSION="${HERMES_VERSION}"

# Set the working directory in the container
WORKDIR /app

# Install HermesTrader-specific system dependencies
# (the official image already has Python 3, git, Node.js, etc.)
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY . /app

# Install HermesTrader-specific Python packages
# (the official image uses `uv` as the package manager)
RUN uv pip install --system --no-cache \
    fastapi \
    uvicorn \
    watchfiles \
    sqlalchemy \
    "psycopg[binary]" \
    xgboost \
    pandas \
    numpy \
    requests \
    "mcp[cli]"

# Set environment variables
ENV PYTHONPATH=/app
ENV HERMES_DSN="postgresql+psycopg://hermes:hermes@db:5432/hermes"
ENV HERMES_WATCHLIST="AAPL,SPY,QQQ,NVDA,AMD,KO"

# The command to run the application is defined in docker-compose.yml
