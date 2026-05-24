# ── Stage 1: Build dependency wheels ──────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System dependencies needed ONLY to compile/build python packages:
#   libpq-dev + gcc + git  — required to compile psycopg binary wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements manifests
COPY requirements.txt /build/
COPY requirements-ml.txt /build/

# Build wheel files for all production dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip wheel --no-cache-dir --wheel-dir=/build/wheels \
        -r requirements.txt \
        -r requirements-ml.txt


# ── Stage 2: Final lightweight runtime container ─────────────────────
FROM python:3.11-slim AS runner

ARG HERMES_VERSION=dev
LABEL hermes.version="${HERMES_VERSION}"
ENV HERMES_VERSION="${HERMES_VERSION}"

WORKDIR /app

# Runtime system libraries:
#   libpq5               — runtime postgres client library (no dev headers needed)
#   tzdata               — populates /usr/share/zoneinfo for python zoneinfo fallback
#   libfreetype6         — required by matplotlib's Agg renderer
#   libpng-dev           — PNG encoder for matplotlib savefig()
#   fontconfig           — matplotlib system font discoverer
#   fonts-dejavu-core    — fallback font set
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        tzdata \
        libfreetype6 \
        libpng-dev \
        fontconfig \
        fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Copy pre-compiled wheels from builder stage
COPY --from=builder /build/wheels /app/wheels

# Install the wheels locally (without compilers or downloading)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --no-index --find-links=/app/wheels /app/wheels/*.whl && \
    rm -rf /app/wheels

# Verify the chart vision layer is importable at build time so a missing
# dependency fails the build rather than silently degrading at runtime.
RUN python -c "import matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot"

# Copy application source code
COPY . /app

# Runtime environment
ENV PYTHONPATH=/app
ENV HERMES_DSN="postgresql+psycopg://hermes:hermes@db:5432/hermes"
ENV HERMES_WATCHLIST="AAPL,SPY,QQQ,NVDA,AMD,KO"
ENV TZ="America/New_York"
