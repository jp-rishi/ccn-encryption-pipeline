#!/bin/bash

################################################################################
# This script is intended to be sourced by other scripts.
# It contains functions for logging and locking.
################################################################################

acquire_lock() {
  local lock_file=$1
  local pid=$$
  local max_attempts=5
  local stale_threshold=300  # seconds
  # Check for a reentrant lock: if the owner is already our PID, return success.
  if [ -f "$lock_file" ]; then
    local current_holder
    current_holder=$(cat "$lock_file")
    if [ "$current_holder" = "$pid" ]; then
      log_message "DEBUG" "locks" "Reentrant acquisition: already hold lock on $lock_file as PID $pid."
      return 0
    fi
  fi

  for ((i = 1; i <= max_attempts; i++)); do
    if (set -o noclobber; echo "$pid" > "$lock_file") 2>/dev/null; then
      log_message "DEBUG" "locks" "Lock acquired on $lock_file by PID $pid (attempt $i)"
      return 0
    fi
    if [ -f "$lock_file" ]; then
      local current_holder
      current_holder=$(cat "$lock_file")
      log_message "DEBUG" "locks" "Lock on $lock_file held by PID $current_holder (attempt $i)"
      
      # Check the age of the lock file.
      local lock_mtime
      if lock_mtime=$(stat -c %Y "$lock_file" 2>/dev/null); then
        local current_time
        current_time=$(date +%s)
        local age=$(( current_time - lock_mtime ))
        if [ $age -gt $stale_threshold ]; then
          log_message "DEBUG" "locks" "Lock on $lock_file is older than ${stale_threshold}s (age ${age}s); removing stale lock."
          rm -f "$lock_file"
          continue 
        fi
      fi

      if ! kill -0 "$current_holder" 2>/dev/null; then
        log_message "DEBUG" "locks" "Stale lock found on $lock_file held by PID $current_holder. Removing stale lock."
        rm -f "$lock_file"
        continue
      fi
    fi
    sleep 2
  done
  log_message "ERROR" "locks" "Failed to acquire lock on $lock_file after $max_attempts attempts."
  send_webhook_notification "Critical: Failed to acquire lock on $lock_file after $max_attempts attempts." || true
  exit 1
}

release_lock() {
  local lock_file=$1
  local pid=$$
  if [ -f "$lock_file" ]; then
    if [ "$(cat "$lock_file")" == "$pid" ]; then
      rm -f "$lock_file"
      log_message "DEBUG" "locks" "Lock released on $lock_file by PID $pid."
    else
      log_message "WARN" "locks" "PID $pid attempted to release $lock_file, but it is held by $(cat "$lock_file")."
      send_webhook_notification "Warning: PID $pid attempted to release $lock_file, but it is held by $(cat "$lock_file")."
    fi
  else
    log_message "WARN" "locks" "Lock file $lock_file does not exist while releasing."
  fi
}
