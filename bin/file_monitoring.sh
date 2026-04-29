#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

################################################################################
#
# This script monitors a designated directory for new files,
# maintains a JSON-based file queue, and dispatches processing jobs.
#
################################################################################

# Trap SIGINT, SIGTERM, and EXIT for graceful termination and cleanup.
trap 'cleanup_and_exit' SIGINT SIGTERM EXIT

#########################
# Configuration & Setup #
#########################

# Ensure BASE_APP_DIR is set.
if [ -z "${BASE_APP_DIR:-}" ]; then
  echo "Error: BASE_APP_DIR is not set. Please set it to the base directory of your application."
  exit 1
fi

# Verify required external tools exist.
for cmd in curl inotifywait jq; do
  if ! command -v "$cmd" &> /dev/null; then
    echo "Error: $cmd is not installed. Please install it and try again."
    exit 1
  fi
done

# Load configuration.
source "$BASE_APP_DIR/config/config.env" || { echo "Failed to source config.env"; exit 1; }

# Override LOG_FILE for monitoring.
LOG_FILE="$BASE_LOG_DIR/file_monitoring.log"

# Load libraries.
source "$BASE_APP_DIR/bin/includes/logging.sh" || { echo "Failed to source logging.sh"; exit 1; }
source "$BASE_APP_DIR/bin/includes/notify.sh" || { log_message "ERROR" "process_csv" "Failed to source notify.sh"; exit 1; }
source "$BASE_APP_DIR/bin/includes/locks.sh" || { log_message "ERROR" "process_csv" "Failed to source locks.sh"; exit 1; }
source "$BASE_APP_DIR/bin/includes/utils.sh" || { log_message "ERROR" "process_csv" "Failed to source utils.sh"; exit 1; }
source "$BASE_APP_DIR/bin/includes/queue_manager.sh" || { log_message "ERROR" "process_csv" "Failed to source queue_manager.sh"; exit 1; }
source "$BASE_APP_DIR/bin/includes/temp_tracker.sh" || { log_message "ERROR" "process_csv" "Failed to source temp_tracker.sh"; exit 1; }
source "$BASE_APP_DIR/bin/includes/state_manager.sh" || { log_message "ERROR" "process_csv" "Failed to source state_manager.sh"; exit 1; }
source "$BASE_APP_DIR/bin/includes/metrics.sh" || { log_message "ERROR" "process_csv" "Failed to source metrics.sh"; exit 1; }

# Validate required configuration variables.
validate_config() {
  local required_vars=("MONITOR_DIR" "PROCESSING_SCRIPT" "BASE_LOG_DIR" "WEBHOOK_URL")
  for var in "${required_vars[@]}"; do
    if [ -z "${!var:-}" ]; then
      log_message "ERROR" "file_monitoring" "$var is not set. Exiting."
      exit 1
    fi
  done
}
validate_config

# Ensure BASE_LOG_DIR exists.
if [ ! -d "$BASE_LOG_DIR" ]; then
  log_message "WARN" "file_monitoring" "BASE_LOG_DIR does not exist. Creating it."
  mkdir -p "$BASE_LOG_DIR" || { log_message "ERROR" "file_monitoring" "Failed to create BASE_LOG_DIR. Exiting."; exit 1; }
fi

# Redirect stderr to error log.
exec 2>>"$BASE_LOG_DIR/file_monitoring_stderr.log"

#########################
# Global Variables      #
#########################
QUEUE_FILE="$BASE_LOG_DIR/file_queue.json"
TEMP_FILE_TRACKER="$BASE_LOG_DIR/temp_files.json"
METRICS_LOG_FILE="$BASE_LOG_DIR/metrics.log"
QUEUE_CHECK_INTERVAL=10
HEALTH_CHECK_INTERVAL=1000
TEMP_FILE_TIMEOUT=3600
FILE_EXTENSION=${FILE_EXTENSION:-csv}  # Default extension: csv
RUNNING=true

####################################
# Directory Monitor Function
####################################
monitor_directory() {
  inotifywait -m "$MONITOR_DIR" -e create -e close_write -e moved_to --format '%w%f' 2>>"$BASE_LOG_DIR/file_monitoring_stderr.log" | while read -r file; do
    if [[ "$file" == *."$FILE_EXTENSION" ]]; then
      log_message "INFO" "file_monitoring" "Detected new file: $file"
      add_to_queue "$file"
    elif [[ "$file" == *.tmp || "$file" == *.filepart ]]; then
      log_message "INFO" "file_monitoring" "Detected temporary file: $file"
      update_temp_file_tracker "$file"
    else
      log_message "INFO" "file_monitoring" "Ignored file: $file"
    fi
  done
}

####################################
# Main Routine
####################################
main() {
  # Clean up any stale temporary files from previous runs.
  cleanup_tmp_files
  initialize_json_file "$QUEUE_FILE"
  initialize_json_file "$TEMP_FILE_TRACKER"
  reload_state
  reset_processing_files
  scan_existing_files

  # Start background processes and capture their errors.
  monitor_directory &
  process_queue &
  log_metrics &
  cleanup_temp_file_tracker_periodically &
  check_deleted_files &
  check_failed_files &
  save_state_periodically &

  wait
}

main
