#!/bin/bash

################################################################################
# This script is supposed to be sourced by file_monitoring.sh 
# to track a queue of temporary files to process.
################################################################################

# Ensure locks.sh is sourced in parent script
. "$BASE_APP_DIR/bin/includes/locks.sh"
# Ensure utils.sh is sourced in parent script
. "$BASE_APP_DIR/bin/includes/utils.sh"

####################################
# Update Temp File Tracker File
####################################
update_temp_file_tracker() {
  local file=$1
  local timestamp=$(date +%s)
  if jq -e --arg file "$file" '.[] | select(.file == $file)' "$TEMP_FILE_TRACKER" &>/dev/null; then
    log_message "DEBUG" "temp_tracker" "Temporary file already tracked: $file"
  else
    if jq --arg file "$file" --argjson timestamp "$timestamp" '. + [{"file": $file, "timestamp": $timestamp}]' "$TEMP_FILE_TRACKER" > "$TEMP_FILE_TRACKER.tmp"; then
      if mv "$TEMP_FILE_TRACKER.tmp" "$TEMP_FILE_TRACKER"; then
        log_message "DEBUG" "temp_tracker" "Added temporary file to tracker: $file"
      else
        log_message "ERROR" "temp_tracker" "Failed to rename $TEMP_FILE_TRACKER.tmp to $TEMP_FILE_TRACKER in update_temp_file_tracker for $file"
        send_webhook_notification "Critical: Failed to update temp file tracker for $file"
        rm -f "$TEMP_FILE_TRACKER.tmp"
      fi
    else
      log_message "ERROR" "temp_tracker" "jq failed to update temp file tracker for $file"
      send_webhook_notification "Critical: jq failed to update temp file tracker for $file"
      rm -f "$TEMP_FILE_TRACKER.tmp"
    fi
  fi
}

####################################
# Clean up temp file tracker
####################################
cleanup_temp_file_tracker() {
  local current_time=$(date +%s)
  local stale_files=()
  local removed_files=()
  acquire_lock "$TEMP_FILE_TRACKER.lock"
  local entries=$(jq -c '.[]' "$TEMP_FILE_TRACKER")
  while read -r entry; do
    local file ts
    file=$(echo "$entry" | jq -r '.file')
    ts=$(echo "$entry" | jq -r '.timestamp')
    log_message "DEBUG" "temp_tracker" "Checking file: $file with timestamp: $ts"
    if [ ! -f "$file" ]; then
      log_message "DEBUG" "temp_tracker" "File $file no longer exists. Adding to removed_files."
      removed_files+=("$file")
      continue
    fi
    if (( current_time - ts > TEMP_FILE_TIMEOUT )); then
      log_message "WARN" "temp_tracker" "File $file is stale. Adding to stale_files."
      stale_files+=("$file")
    fi
  done <<< "$entries"
  for file in "${removed_files[@]}"; do
    log_message "DEBUG" "temp_tracker" "Removing $file from tracker."
    if jq --arg file "$file" 'del(.[] | select(.file == $file))' "$TEMP_FILE_TRACKER" > "$TEMP_FILE_TRACKER.tmp"; then
      if mv "$TEMP_FILE_TRACKER.tmp" "$TEMP_FILE_TRACKER"; then
        log_message "DEBUG" "temp_tracker" "Removed $file from tracker."
      else
        log_message "ERROR" "temp_tracker" "Failed to rename temp tracker file while removing $file"
        send_webhook_notification "Critical: Failed to remove file from temp tracker for $file"
        rm -f "$TEMP_FILE_TRACKER.tmp"
      fi
    else
      log_message "ERROR" "temp_tracker" "jq failed to remove $file from temp tracker."
      send_webhook_notification "Critical: jq failed to remove file from temp tracker for $file"
      rm -f "$TEMP_FILE_TRACKER.tmp"
    fi
  done
  for file in "${stale_files[@]}"; do
    send_webhook_notification "Temporary file $file has been stale for more than $TEMP_FILE_TIMEOUT seconds. Please take action."
  done
  release_lock "$TEMP_FILE_TRACKER.lock"
}

cleanup_temp_file_tracker_periodically() {
  while [ "$RUNNING" = true ]; do
    cleanup_temp_file_tracker
    sleep "$HEALTH_CHECK_INTERVAL" # Expected to be defined in parent script
  done
}
