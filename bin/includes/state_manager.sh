#!/bin/bash

################################################################################
# This script is sourced by file_monitoring.sh.
# This script contains functions for managing and monitoring the state of 
# critical JSON files which are used by the file_monitoring.sh to maintain 
# a queue of files to process.
################################################################################

# reload_state: Reload state from backup files.
reload_state() {
  if [ -f "$QUEUE_FILE.bak" ]; then
    cp "$QUEUE_FILE.bak" "$QUEUE_FILE"
    log_message "INFO" "state_manager" "Reloaded queue state from backup."
  else
    log_message "WARN" "state_manager" "Queue backup file missing. Cannot reload queue state."
  fi
  
  if [ -f "$TEMP_FILE_TRACKER.bak" ]; then
    cp "$TEMP_FILE_TRACKER.bak" "$TEMP_FILE_TRACKER"
    log_message "INFO" "state_manager" "Reloaded temporary file tracker state from backup."
  else
    log_message "WARN" "state_manager" "Temporary file tracker backup file missing. Cannot reload tracker state."
  fi
}

# save_state_periodically: Regularly verify and back up the critical JSON files.
# Also, perform a health check: if a file is missing, reinitialize it and send a webhook notification.
save_state_periodically() {
  trap 'RUNNING=false' SIGINT SIGTERM
  while [ "$RUNNING" = true ]; do
    # Health-check and reinitialize if needed.
    if [ ! -f "$QUEUE_FILE" ]; then
      log_message "WARN" "state_manager" "Queue file $QUEUE_FILE missing during periodic save. Reinitializing."
      initialize_json_file "$QUEUE_FILE"
    fi

    if [ ! -f "$TEMP_FILE_TRACKER" ]; then
      log_message "WARN" "state_manager" "Temporary file tracker $TEMP_FILE_TRACKER missing during periodic save. Reinitializing."
      initialize_json_file "$TEMP_FILE_TRACKER"
    fi

    # Backup the files.
    if [ -f "$QUEUE_FILE" ]; then
      cp "$QUEUE_FILE" "$QUEUE_FILE.bak"
    else
      log_message "WARN" "state_manager" "Queue file $QUEUE_FILE does not exist during backup."
    fi

    if [ -f "$TEMP_FILE_TRACKER" ]; then
      cp "$TEMP_FILE_TRACKER" "$TEMP_FILE_TRACKER.bak"
    else
      log_message "WARN" "state_manager" "Temporary file tracker $TEMP_FILE_TRACKER does not exist during backup."
    fi

    log_message "DEBUG" "state_manager" "State saved successfully to backup files."
    sleep "$HEALTH_CHECK_INTERVAL" # Value expected to be defined in file_monitoring.sh.
  done
}
