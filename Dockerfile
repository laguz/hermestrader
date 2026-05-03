# ── HermesTrader — single-stage production image ──────────────────────
FROM python:3.11-slim

ARG HERMES_VERSION=dev
LABEL hermes.version="${HERMES_VERSION}"
ENV HERMES_VERSION="${HERMES_VERSION}"

WORKDIR /app

# System deps:
#   libpq-dev + gcc  — needed by psycopg binary build
#   tzdata           — populates /usr/share/zoneinfo for Python's zoneinfo module
#                      (market_hours.py uses ZoneInfo("America/New_York"))
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer-cache friendly).
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source.
COPY . /app

# Runtime environment
ENV PYTHONPATH=/app
ENV HERMES_DSN="postgresql+psycopg://hermes:hermes@db:5432/hermes"
ENV HERMES_WATCHLIST="AAPL,SPY,QQQ,NVDA,AMD,KO"
ENV TZ="America/New_York"
