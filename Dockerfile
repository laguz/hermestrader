# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
# (Since we don't have a requirements.txt, we'll install them directly)
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn \
    sqlalchemy \
    psycopg[binary] \
    xgboost \
    pandas \
    numpy

# Set environment variables
ENV PYTHONPATH=/app
ENV HERMES_DSN="postgresql+psycopg://hermes:hermes@db:5432/hermes"
ENV HERMES_WATCHLIST="AAPL,SPY,QQQ,NVDA,AMD,KO"

# The command to run the application is defined in docker-compose.yml
