#!/bin/bash

################################################################################
# This script manages a JSON-based file queue.
# It is used by file_monitoring.sh to maintain a queue of files to process.
################################################################################

# Ensure locks.sh is sourced
. "$BASE_APP_DIR/bin/includes/locks.sh"

####################################
# Update Temp File Tracker File
####################################
update_temp_file_tracker() {
  local file=$1
  local timestamp
  timestamp=$(date +%s)
  if jq -e --arg file "$file" '.[] | select(.file == $file)' "$TEMP_FILE_TRACKER" &>/dev/null; then
    log_message "DEBUG" "queue_manager" "Temporary file already tracked: $file"
  else
    if jq --arg file "$file" --argjson timestamp "$timestamp" '. + [{"file": $file, "timestamp": $timestamp}]' "$TEMP_FILE_TRACKER" > "$TEMP_FILE_TRACKER.tmp"; then
      if mv "$TEMP_FILE_TRACKER.tmp" "$TEMP_FILE_TRACKER"; then
        log_message "DEBUG" "queue_manager" "Added temporary file to tracker: $file"
      else
        log_message "ERROR" "queue_manager" "Failed to rename $TEMP_FILE_TRACKER.tmp to $TEMP_FILE_TRACKER in update_temp_file_tracker for $file"
        send_webhook_notification "Critical: Failed to update temp file tracker for $file"
        rm -f "$TEMP_FILE_TRACKER.tmp"
      fi
    else
      log_message "ERROR" "queue_manager" "jq failed to update temp file tracker for $file"
      send_webhook_notification "Critical: jq failed to update temp file tracker for $file"
      rm -f "$TEMP_FILE_TRACKER.tmp"
    fi
  fi
}

####################################
# Add file to queue
####################################
add_to_queue() {
  local file=$1
  if [ -z "$file" ]; then
    log_message "ERROR" "queue_manager" "Cannot add to queue: file is empty."
    return 1
  fi
  acquire_lock "$QUEUE_FILE.lock"
  if jq -e --arg file "$file" '.[] | select(.file == $file)' "$QUEUE_FILE" &>/dev/null; then
    log_message "DEBUG" "queue_manager" "File already in queue: $file"
  else
    if jq --arg file "$file" '. + [{"file": $file, "status": "pending"}]' "$QUEUE_FILE" > "$QUEUE_FILE.tmp"; then
      if mv "$QUEUE_FILE.tmp" "$QUEUE_FILE"; then
        log_message "INFO" "queue_manager" "Added file to queue: $file"
      else
        log_message "ERROR" "queue_manager" "Failed to rename $QUEUE_FILE.tmp to $QUEUE_FILE in add_to_queue for $file"
        send_webhook_notification "Critical: Failed to update file queue for $file"
        rm -f "$QUEUE_FILE.tmp"
      fi
    else
      log_message "ERROR" "queue_manager" "jq failed to update the queue file for $file"
      send_webhook_notification "Critical: jq failed to update file queue for $file"
      rm -f "$QUEUE_FILE.tmp"
    fi
  fi
  release_lock "$QUEUE_FILE.lock"
}

####################################
# Update file queue status.
####################################
update_queue_file() {
  local jq_filter=$1
  local file=$2
  
  # Before performing the update, check if the queue file exists.
  if [ ! -f "$QUEUE_FILE" ]; then
    log_message "WARN" "queue_manager" "Queue file missing. Reinitializing."
    initialize_json_file "$QUEUE_FILE"
  fi
  
  acquire_lock "$QUEUE_FILE.lock"
  if jq --arg file "$file" "$jq_filter" "$QUEUE_FILE" > "$QUEUE_FILE.tmp"; then
    if mv "$QUEUE_FILE.tmp" "$QUEUE_FILE"; then
      log_message "INFO" "queue_manager" "Queue file updated for file: $file"
    else
      log_message "ERROR" "queue_manager" "Failed to rename tmp file in update_queue_file for $file"
      send_webhook_notification "Critical: FAILED to update queue file for $file"
      rm -f "$QUEUE_FILE.tmp"
    fi
  else
    log_message "ERROR" "queue_manager" "jq failed to update queue file for $file"
    send_webhook_notification "Critical: jq FAILED to update queue file for $file"
    rm -f "$QUEUE_FILE.tmp"
    release_lock "$QUEUE_FILE.lock"
    return 1
  fi
  release_lock "$QUEUE_FILE.lock"
}

####################################
# Process File in Queue Function
####################################
process_queue() {
  trap 'RUNNING=false' SIGINT SIGTERM
  local last_state="non-empty"
  while [ "$RUNNING" = true ]; do
    # Find a pending file.
    acquire_lock "$QUEUE_FILE.lock"
    local file
    file=$(jq -r '.[] | select(.status == "pending") | .file' "$QUEUE_FILE" | head -n 1)
    release_lock "$QUEUE_FILE.lock"

    if [ -z "$file" ]; then
      if [ "$last_state" != "empty" ]; then
        log_message "DEBUG" "queue_manager" "No pending files in the queue."
        last_state="empty"
      fi
      sleep "$QUEUE_CHECK_INTERVAL"
      continue
    fi

    log_message "INFO" "queue_manager" "Processing file: $file"

    # Mark file as processing.
    acquire_lock "$QUEUE_FILE.lock"
    if jq --arg file "$file" 'map(if .file == $file then .status = "processing" else . end)' "$QUEUE_FILE" > "$QUEUE_FILE.tmp"; then
      if mv "$QUEUE_FILE.tmp" "$QUEUE_FILE"; then
        log_message "DEBUG" "queue_manager" "Marked $file as processing."
      else
        log_message "ERROR" "queue_manager" "Failed to rename queue file while marking processing for $file"
        send_webhook_notification "Critical: Failed to update queue file for $file" || true
        rm -f "$QUEUE_FILE.tmp"
      fi
    else
      log_message "ERROR" "queue_manager" "jq failed to mark $file as processing"
      send_webhook_notification "Critical: jq failed update on queue file for $file" || true
      rm -f "$QUEUE_FILE.tmp"
    fi
    release_lock "$QUEUE_FILE.lock"

    # Run the processing script.
    set +e
    bash "$PROCESSING_SCRIPT" "$file"
    local status=$?
    set -e
    log_message "DEBUG" "queue_manager" "Processing script for $file exited with status: $status"

    #
    # Begin update/cleanup area.
    # We acquire the lock before updating the queue
    # and then execute the update code inside a block so that even
    # if a command fails, we always call release_lock.
    #
    acquire_lock "$QUEUE_FILE.lock"
    {
      if [ $status -eq 0 ]; then
        log_message "INFO" "queue_manager" "Successfully processed file: $file"
        # Mark file as processed.
        if jq --arg file "$file" 'map(if .file == $file then .status = "processed" else . end)' "$QUEUE_FILE" > "$QUEUE_FILE.tmp"; then
          if mv "$QUEUE_FILE.tmp" "$QUEUE_FILE"; then
            log_message "DEBUG" "queue_manager" "Marked $file as processed."
          else
            log_message "ERROR" "queue_manager" "Failed to rename queue file while marking processed for $file"
            send_webhook_notification "Critical: Failed to update queue file for $file" || true
            rm -f "$QUEUE_FILE.tmp"
          fi
        else
          log_message "ERROR" "queue_manager" "jq failed to mark $file as processed"
          send_webhook_notification "Critical: jq failed to update queue for $file" || true
          rm -f "$QUEUE_FILE.tmp"
        fi
        # Remove the processed file from the queue.
        if jq --arg file "$file" 'map(select(.file != $file))' "$QUEUE_FILE" > "$QUEUE_FILE.tmp"; then
          if mv "$QUEUE_FILE.tmp" "$QUEUE_FILE"; then
            log_message "DEBUG" "queue_manager" "Removed $file from queue."
          else
            log_message "ERROR" "queue_manager" "Failed to remove $file from queue during rename."
            send_webhook_notification "Critical: Failed to remove file from queue for $file" || true
            rm -f "$QUEUE_FILE.tmp"
          fi
        else
          log_message "ERROR" "queue_manager" "jq failed to remove $file from queue"
          send_webhook_notification "Critical: jq failed to remove file from queue for $file" || true
          rm -f "$QUEUE_FILE.tmp"
        fi
        # Update the metrics.
        increment_processed_count_in_metrics
      else
        log_message "ERROR" "queue_manager" "Failed to process file: $file"
        # Mark file as failed.
        if jq --arg file "$file" 'map(if .file == $file then .status = "failed" else . end)' "$QUEUE_FILE" > "$QUEUE_FILE.tmp"; then
          if mv "$QUEUE_FILE.tmp" "$QUEUE_FILE"; then
            log_message "DEBUG" "queue_manager" "Marked $file as failed."
          else
            log_message "ERROR" "queue_manager" "Failed to rename queue file while marking failed for $file"
            send_webhook_notification "Critical: Failed to update queue file for $file" || true
            rm -f "$QUEUE_FILE.tmp"
          fi
        else
          log_message "ERROR" "queue_manager" "jq failed to mark $file as failed"
          send_webhook_notification "Critical: jq failed to update queue for $file" || true
          rm -f "$QUEUE_FILE.tmp"
        fi
        send_webhook_notification "Processing failed for file: $file" || true
      fi
    } || true
    # Always release the lock regardless of errors.
    release_lock "$QUEUE_FILE.lock"
    #
    # End update/cleanup area.
    #

    last_state="non-empty"
  done
}


####################################
# Scan Existing Files
####################################
scan_existing_files() {
  trap 'RUNNING=false' SIGINT SIGTERM
  while [ "$RUNNING" = true ]; do
    log_message "INFO" "queue_manager" "Scanning directory for existing .$FILE_EXTENSION files..."
    
    # Use find to list matching files, then stat to get modification times,
    # sort them and finally process each file in order.
    find "$MONITOR_DIR" -maxdepth 1 -type f -name "*.$FILE_EXTENSION" -print0 2>/dev/null | \
      xargs -0 stat --format '%Y %n' 2>/dev/null | sort -n | cut -d' ' -f2- | \
      while IFS= read -r file; do
        if [ ! -e "$file" ]; then
          continue
        fi
        
        # Attempt to acquire the lock.
        if ! acquire_lock "$QUEUE_FILE.lock"; then
          log_message "ERROR" "queue_manager" "Failed to acquire lock on $QUEUE_FILE.lock while scanning $file"
          continue
        fi

        # Check if the file is already in the queue.
        if jq -e --arg file "$file" '.[] | select(.file == $file)' "$QUEUE_FILE" &>/dev/null; then
          release_lock "$QUEUE_FILE.lock"
          log_message "DEBUG" "queue_manager" "File already in queue: $file"
        else
          log_message "INFO" "queue_manager" "Adding existing file to queue: $file"
          add_to_queue "$file"
        fi
      done

    sleep "$HEALTH_CHECK_INTERVAL"
  done
}


####################################
# Check Deleted Files
####################################
check_deleted_files() {
  while [ "$RUNNING" = true ]; do
    acquire_lock "$QUEUE_FILE.lock"
    jq -c '.[]' "$QUEUE_FILE" | while read -r entry; do
      local file
      file=$(echo "$entry" | jq -r '.file')
      if [ ! -f "$file" ]; then
        log_message "INFO" "queue_manager" "File $file not found. Removing from queue."
        if jq --arg file "$file" 'map(select(.file != $file))' "$QUEUE_FILE" > "$QUEUE_FILE.tmp"; then
          if mv "$QUEUE_FILE.tmp" "$QUEUE_FILE"; then
            log_message "DEBUG" "queue_manager" "Removed $file from queue."
          else
            log_message "ERROR" "queue_manager" "Failed to remove $file from queue (rename error)."
            send_webhook_notification "Critical: Failed to remove file from queue for $file"
            rm -f "$QUEUE_FILE.tmp"
          fi
        else
          log_message "ERROR" "queue_manager" "jq failed to remove $file from queue."
          send_webhook_notification "Critical: jq failed to remove file from queue for $file"
          rm -f "$QUEUE_FILE.tmp"
        fi
      fi
    done
    release_lock "$QUEUE_FILE.lock"
    sleep 60
  done
}

####################################
# Check Failed Files
####################################
check_failed_files() {
  while [ "$RUNNING" = true ]; do
    acquire_lock "$QUEUE_FILE.lock"
    jq -c '.[] | select(.status == "failed")' "$QUEUE_FILE" | while read -r entry; do
      local file
      file=$(echo "$entry" | jq -r '.file')
      if [ -f "$file" ]; then
        log_message "WARN" "queue_manager" "Failed file still exists: $file"
        send_webhook_notification "Failed file $file still exists in the monitored directory. Please take action (e.g., delete or reprocess the file)."
      fi
    done
    release_lock "$QUEUE_FILE.lock"
    sleep 3600
  done
}

####################################
# Reset Processing Files
####################################
reset_processing_files() {
  acquire_lock "$QUEUE_FILE.lock"
  if jq 'map(if .status == "processing" then .status = "pending" else . end)' "$QUEUE_FILE" > "$QUEUE_FILE.tmp"; then
    if mv "$QUEUE_FILE.tmp" "$QUEUE_FILE"; then
      log_message "INFO" "queue_manager" "Reset processing files to pending."
    else
      log_message "ERROR" "queue_manager" "Failed to rename queue file while resetting processing files."
      send_webhook_notification "Critical: Failed to update queue file during reset."
      rm -f "$QUEUE_FILE.tmp"
    fi
  else
    log_message "ERROR" "queue_manager" "jq failed to reset processing files."
    send_webhook_notification "Critical: jq failed to reset processing files."
    rm -f "$QUEUE_FILE.tmp"
  fi
  release_lock "$QUEUE_FILE.lock"
}