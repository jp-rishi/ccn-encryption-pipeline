#!/bin/bash

# Check that required dependencies are available
for cmd in curl jq; do
  if ! command -v "$cmd" &> /dev/null; then
    echo "Error: $cmd is not installed. Please install it and try again."
    exit 1
  fi
done

# Confirm that WEBHOOK_URL is set
if [[ -z "$WEBHOOK_URL" ]]; then
  echo "WEBHOOK_URL environment variable is not set; aborting." >&2
  exit 1
fi

# Constants for retries
readonly RETRY_COUNT=3
readonly RETRY_DELAY=5
readonly TIMEOUT=10

###################
# Webhook Function        
###################
# This function sends a message to a specified webhook URL.
send_webhook_notification() {
  local message="$1"
  
  # Validate that the message is not empty.
  if [[ -z "$message" ]]; then
    log_message "ERROR" "file_monitoring" "No message provided to send_webhook_notification."
    return 1
  fi

  local payload
  payload=$(jq -n --arg text "$message" '{
    type: "message",
    attachments: [
      {
        contentType: "application/vnd.microsoft.card.adaptive",
        content: {
          type: "AdaptiveCard",
          body: [
            { type: "TextBlock", text: $text, weight: "bolder", size: "medium" }
          ],
          msteams: { width: "Full" }
        }
      }
    ]
  }')

  local attempt
  for (( attempt = 1; attempt <= RETRY_COUNT; attempt++ )); do
    # Using --fail ensures that curl returns a non-zero code for HTTP errors.
    if curl -s --fail --max-time "$TIMEOUT" -X POST -H 'Content-Type: application/json' -d "$payload" "$WEBHOOK_URL"; then
      log_message "DEBUG" "file_monitoring" "Webhook notification sent successfully on attempt ${attempt}."
      return 0
    fi
    log_message "WARN" "file_monitoring" "Attempt ${attempt} failed to send webhook notification. Retrying in ${RETRY_DELAY} seconds..."
    sleep "$RETRY_DELAY"
  done

  log_message "ERROR" "file_monitoring" "Failed to send webhook notification after ${RETRY_COUNT} attempts."
  return 1
}
