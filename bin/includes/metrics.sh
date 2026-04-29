#!/bin/bash

################################################################################
# This script is intended to be sourced by other scripts.
# It contains functions for logging and updating metrics.
################################################################################

increment_processed_count_in_metrics() {
  local metrics_file="$METRICS_LOG_FILE"
  local lock_file="$METRICS_LOG_FILE.lock"

  acquire_lock "$lock_file"
  if [[ ! -f "$metrics_file" ]]; then
    log_message "WARN" "metrics" "Metrics log file does not exist. Creating a new one."
    echo "{\"timestamp\":\"$(date '+%Y-%m-%dT%H:%M:%SZ')\",\"processed\":0,\"failed\":0,\"queue_size\":0}" > "$metrics_file"
  fi
  local last_metrics=$(tail -n 1 "$metrics_file")
  local current_processed=$(echo "$last_metrics" | jq -r '.processed // 0')
  local new_processed=$((current_processed + 1))
  local timestamp=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "{\"timestamp\":\"$timestamp\",\"processed\":$new_processed,\"failed\":$(echo "$last_metrics" | jq -r '.failed // 0'),\"queue_size\":$(echo "$last_metrics" | jq -r '.queue_size // 0')}" >> "$metrics_file"
  log_message "DEBUG" "metrics" "Updated processed count: $new_processed"
  release_lock "$lock_file"
}

log_metrics() {
  trap 'RUNNING=false' SIGINT SIGTERM
  while [ "$RUNNING" = true ]; do
    if [[ ! -f "$QUEUE_FILE" ]] || ! jq . "$QUEUE_FILE" &>/dev/null; then
      log_message "ERROR" "metrics" "Queue file is missing or invalid: $QUEUE_FILE"
      sleep "$HEALTH_CHECK_INTERVAL"
      continue
    fi
    local failed_count=$(jq '[.[] | select(.status == "failed")] | length' "$QUEUE_FILE")
    local queue_size=$(jq '[.[] | select(.status == "pending" or .status == "processing")] | length' "$QUEUE_FILE")
    local processed_count
    if [[ -f "$METRICS_LOG_FILE" ]]; then
      processed_count=$(tail -n 1 "$METRICS_LOG_FILE" | jq -r '.processed // 0')
    else
      processed_count=0
    fi
    log_message "DEBUG" "metrics" "Queue: Processed: $processed_count, Failed: $failed_count, Queue Size: $queue_size"
    local timestamp=$(date '+%Y-%m-%dT%H:%M:%SZ')
    echo "{\"timestamp\":\"$timestamp\",\"processed\":$processed_count,\"failed\":$failed_count,\"queue_size\":$queue_size}" >> "$METRICS_LOG_FILE"
    sleep "$HEALTH_CHECK_INTERVAL"
  done
}
