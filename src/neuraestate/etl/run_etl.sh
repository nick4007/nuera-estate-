#!/usr/bin/env sh
# src/neuraestate/etl/run_etl.sh
# Minimal, portable POSIX shell runner (works inside Docker).
# It runs the ETL script, logs output to /app/logs/etl_runs.log and sleeps.

ETL_SCRIPT_PATH="${ETL_SCRIPT_PATH:-src/neuraestate/pipelines/preprocess.py}"
SLEEP_SECONDS="${SLEEP_SECONDS:-3600}"
LOG_DIR="${LOG_DIR:-/app/logs}"
LOG_FILE="${LOG_DIR}/etl_runs.log"

mkdir -p "$LOG_DIR"

echo "ETL runner starting. ETL_SCRIPT_PATH=$ETL_SCRIPT_PATH SLEEP_SECONDS=$SLEEP_SECONDS" >> "$LOG_FILE" 2>&1

while true; do
  ts="$(date --utc +'%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date +'%Y-%m-%dT%H:%M:%SZ')"
  echo "[$ts] Starting ETL run" | tee -a "$LOG_FILE"
  if python "$ETL_SCRIPT_PATH" >> "$LOG_FILE" 2>&1; then
    ts2="$(date --utc +'%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date +'%Y-%m-%dT%H:%M:%SZ')"
    echo "[$ts2] ETL run completed successfully" | tee -a "$LOG_FILE"
  else
    ts3="$(date --utc +'%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || date +'%Y-%m-%dT%H:%M:%SZ')"
    echo "[$ts3] ETL run FAILED (exit code != 0). See logs." | tee -a "$LOG_FILE"
  fi

  echo "Sleeping for ${SLEEP_SECONDS} seconds..." | tee -a "$LOG_FILE"
  sleep "${SLEEP_SECONDS}"
done


