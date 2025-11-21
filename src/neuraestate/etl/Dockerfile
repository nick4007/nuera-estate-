# src/neuraestate/etl/Dockerfile
FROM python:3.11-slim

# Install build deps needed by psycopg2
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential gcc libpq-dev curl ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy repo-level requirements file into image (build context is repo root)
COPY requirements.txt /app/requirements.txt

# Install Python deps
RUN pip install --no-cache-dir --upgrade pip \
    && if [ -f /app/requirements.txt ]; then pip install --no-cache-dir -r /app/requirements.txt; fi

# Copy the run script from its real location in the repo into the image
COPY src/neuraestate/etl/run_etl.sh /usr/local/bin/run_etl.sh
RUN chmod +x /usr/local/bin/run_etl.sh

# default env values (override in compose)
ENV ETL_SCRIPT_PATH=src/neuraestate/pipelines/preprocess.py
ENV SLEEP_SECONDS=3600
ENV LOG_DIR=/app/logs

ENTRYPOINT ["/usr/local/bin/run_etl.sh"]



