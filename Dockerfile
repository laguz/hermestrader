# Use a slimmer official Python runtime as a parent image
FROM python:3.11-slim

# Set timezone and non-interactive frontend to prevent apt prompts
ENV DEBIAN_FRONTEND=noninteractive
ENV TZ=UTC

# Build arguments
ARG APP_USER=appuser
ARG APP_UID=1000
ARG APP_GID=1000

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
# We use --no-install-recommends for smaller image size
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    cmake \
    swig \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user and group
RUN groupadd -g ${APP_GID} ${APP_USER} && \
    useradd -u ${APP_UID} -g ${APP_GID} -s /bin/bash -m ${APP_USER}

# Copy the requirements file into the container
COPY requirements.txt .

# Upgrade pip and install packages
RUN pip install --upgrade pip && \
    pip install --no-cache-dir --default-timeout=1000 -r requirements.txt

# Copy the rest of the application code into the container
# We change ownership to the non-root user here
COPY --chown=${APP_USER}:${APP_USER} . .

# Switch over to the non-root user
USER ${APP_USER}

# Expose the port the app runs on
EXPOSE 8080

# Define environment variables
ENV FLASK_APP=app.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=8080
# Python buffering and bytecode settings
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Run the application with gunicorn (production server)
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "--workers", "2", "--timeout", "600", "app:app"]
