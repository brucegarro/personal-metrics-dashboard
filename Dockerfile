# syntax=docker/dockerfile:1.7

# To run this container this command can be used:
#   docker run --rm -it -p 8000:8000 --name fastapi ghcr.io/brucegarro/personal-metrics-dashboard:dev

FROM python:3.12-slim

# Don't write .pyc files, dont buffer cmd line input/output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Set the working directory
WORKDIR /app

# Update apt and built-essential depedencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*


# Install python dependencies
COPY requirements.txt /app/requirements.txt
RUN python -m pip install --upgrade pip && \
    pip install -r /app/requirements.txt

# Copy files from repo into app
COPY . /app

# Create user
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

# Expose the port the app runs on
EXPOSE 8000


