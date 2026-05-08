# ── HermesTrader — single-stage production image ──────────────────────
FROM python:3.11-slim

ARG HERMES_VERSION=dev
LABEL hermes.version="${HERMES_VERSION}"
ENV HERMES_VERSION="${HERMES_VERSION}"

WORKDIR /app

# System deps:
#   libpq-dev + gcc      — needed by psycopg binary build
#   tzdata               — populates /usr/share/zoneinfo for Python's zoneinfo module
#                          (market_hours.py uses ZoneInfo("America/New_York"))
#   libfreetype6         — required by matplotlib's Agg renderer (chart vision layer)
#   libpng-dev           — PNG encode/decode used by matplotlib savefig()
#   fontconfig           — lets matplotlib discover system fonts for axis labels
#   fonts-dejavu-core    — fallback font set; matplotlib warns and skips labels without it
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        tzdata \
        libfreetype6 \
        libpng-dev \
        fontconfig \
        fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer-cache friendly).
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Verify the chart vision layer is importable at build time so a missing
# dependency fails the build rather than silently degrading at runtime.
RUN python -c "import matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot"

# Copy application source.
COPY . /app

# Runtime environment
ENV PYTHONPATH=/app
ENV HERMES_DSN="postgresql+psycopg://hermes:hermes@db:5432/hermes"
ENV HERMES_WATCHLIST="AAPL,SPY,QQQ,NVDA,AMD,KO"
ENV TZ="America/New_York"
