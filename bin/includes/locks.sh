#!/bin/bash


################################################################################
# This script is intended to be sourced by other scripts.
# It contains functions for logging and locking.
################################################################################

acquire_lock() {
  local lock_file=$1
  local pid=$$
  local max_attempts=5
  for ((i = 1; i <= max_attempts; i++)); do
    if (set -o noclobber; echo "$pid" > "$lock_file") 2>/dev/null; then
      log_message "DEBUG" "locks" "Lock acquired on $lock_file by PID $pid (attempt $i)"
      return 0
    fi
    log_message "DEBUG" "locks" "Lock on $lock_file busy (attempt $i); retrying..."
    if [ -f "$lock_file" ] && ! kill -0 "$(cat "$lock_file")" 2>/dev/null; then
      log_message "DEBUG" "locks" "Stale lock found on $lock_file; cleaning up."
      rm -f "$lock_file"
      continue
    fi
    sleep 2
  done
  log_message "ERROR" "locks" "Failed to acquire lock on $lock_file after $max_attempts attempts."
  exit 1
}

release_lock() {
  local lock_file=$1
  local pid=$$
  if [ -f "$lock_file" ]; then
    if [ "$(cat "$lock_file")" == "$pid" ]; then
      rm -f "$lock_file"
    else
      log_message "WARN" "locks" "Cannot release lock on $lock_file. Lock not owned by this process."
    fi
  else
    log_message "WARN" "locks" "Lock file $lock_file does not exist."
  fi
}
