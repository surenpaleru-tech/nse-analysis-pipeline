# ==============================================================================
# NSE Analysis Pipeline — Lightweight Python-only Dockerfile
# Used for Render Cron Jobs (data ingestion + analytics)
# ==============================================================================
FROM python:3.12-slim

# Install system dependencies for PostgreSQL client
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy and install Python dependencies
COPY pyproject.toml .
COPY README.md .
RUN pip install --no-cache-dir -e .

# Copy application source
COPY . .

# Default command — run the daily pipeline
CMD ["python", "run_pipeline.py"]
