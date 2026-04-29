#!/bin/bash


# Ensure BASE_APP_DIR is set and valid
if [ -z "$BASE_APP_DIR" ] || [ ! -d "$BASE_APP_DIR" ]; then
  echo "BASE_APP_DIR is not set or is not a valid directory. Please set it before sourcing this script."
  exit 1
fi

# Load configuration
# shellcheck source=/dev/null
source "$BASE_APP_DIR/config/config.env" || { echo "Failed to source config.env"; exit 1; }

# Ensure logger is available
if ! command -v logger &> /dev/null; then
  echo "The 'logger' command is not available. Please install it to enable syslog logging."
  exit 1
fi

# Ensure BASE_LOG_DIR is set and valid.
if [ -z "$BASE_LOG_DIR" ] || [[ "$BASE_LOG_DIR" =~ [^a-zA-Z0-9/_-] ]]; then
  echo "BASE_LOG_DIR is not set or contains invalid characters. Please set it correctly in config.env."
  exit 1
fi

# Set default log level if not already set.
LOG_LEVEL="${LOG_LEVEL:-INFO}"

# Define log level hierarchy.
declare -A LOG_LEVELS
LOG_LEVELS=(["DEBUG"]=0 ["INFO"]=1 ["WARN"]=2 ["ERROR"]=3 ["METRICS"]=1)

# Use the LOG_FILE variable if set; otherwise default to application.log.
LOG_FILE="${LOG_FILE:-$BASE_LOG_DIR/application.log}"

log_message() {
  local level="$1"
  local component="$2"
  local message="$3"
  local file="${4:-N/A}"       # Default to "N/A" if file not provided.
  local duration="${5:-N/A}"   # Default to "N/A" if duration not provided.
  local timestamp
  timestamp=$(date '+%Y-%m-%dT%H:%M:%SZ')
  
  local log_entry
  log_entry="{\"timestamp\":\"$timestamp\",\"level\":\"$level\",\"component\":\"$component\",\"message\":\"$message\",\"file\":\"$file\",\"duration\":\"$duration\"}"
  
  # Verify valid log level.
  if [[ -z "${LOG_LEVELS[$level]}" ]]; then
    echo "{\"timestamp\":\"$timestamp\",\"level\":\"ERROR\",\"component\":\"logging\",\"message\":\"Invalid log level: $level\"}" \
      | tee -a "$LOG_FILE" | logger -t "$component"
    exit 1
  fi
  
  # Only log messages that meet or exceed the current LOG_LEVEL.
  if (( ${LOG_LEVELS[$level]} >= ${LOG_LEVELS[$LOG_LEVEL]} )); then
    echo "$log_entry" | tee -a "$LOG_FILE" | logger -t "$component"
  fi
}

# Example usage:
# log_message "INFO" "file_monitoring" "This is an info message" "example.csv" "100ms"
