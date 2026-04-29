#!/bin/bash

################################################################################
# This script is supposed to be sourced by file_monitoring.sh
################################################################################


initialize_json_file() {
  local file=$1
  if [ ! -f "$file" ]; then
    log_message "WARN" "utils" "File $file is missing. Attempting to reinitialize it."
    send_webhook_notification "Warning: Critical file $file was deleted and is being reinitialized."
    if echo "[]" > "$file"; then
      log_message "INFO" "utils" "Reinitialized JSON file: $file"
    else
      log_message "ERROR" "utils" "Failed to reinitialize JSON file: $file. Exiting."
      exit 1
    fi
  elif ! jq . "$file" &>/dev/null; then
    log_message "WARN" "utils" "Invalid JSON file: $file. Reinitializing."
    send_webhook_notification "Warning: Critical file $file contains invalid JSON. Reinitializing now."
    if echo "[]" > "$file"; then
      log_message "INFO" "utils" "Reinitialized JSON file: $file"
    else
      log_message "ERROR" "utils" "Failed to reinitialize JSON file: $file. Exiting."
      exit 1
    fi
  else
    log_message "INFO" "utils" "JSON file is valid: $file"
  fi
}

cleanup_tmp_files() {
  rm -f "$QUEUE_FILE.tmp" "$TEMP_FILE_TRACKER.tmp"
  log_message "INFO" "utils" "Cleaned up any leftover .tmp files on startup."
}

cleanup_and_exit() {
  if [ "$RUNNING" = true ]; then
    log_message "INFO" "utils" "Shutting down gracefully."
    RUNNING=false
    [ -f "$QUEUE_FILE.lock" ] && release_lock "$QUEUE_FILE.lock"
    [ -f "$TEMP_FILE_TRACKER.lock" ] && release_lock "$TEMP_FILE_TRACKER.lock"
    pkill -P $$
  fi
}

handle_error() {
  log_message "ERROR" "utils" "An error occurred. Reloading state from backup..."
  reload_state
}
