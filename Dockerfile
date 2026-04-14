# ════════════════════════════════════════════════════════════
#  Single Dockerfile — built once, used by all app containers
#  Each container overrides CMD in docker-compose.yml
# ════════════════════════════════════════════════════════════

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Default command — overridden per container in docker-compose.yml
CMD ["python", "-m", "app.pipeline.main"]