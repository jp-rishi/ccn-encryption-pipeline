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

# Configuration constants with default values.
readonly RETRY_COUNT="${RETRY_COUNT:-3}"
readonly RETRY_DELAY="${RETRY_DELAY:-5}"
readonly TIMEOUT="${TIMEOUT:-10}"
readonly SUCCESS_CODE_PATTERN="^2[0-9][0-9]$"   # Expected successful HTTP codes (e.g., 200, 201, etc.)

#############################
# Function: send_webhook_notification
# Description:
#   Sends a JSON payload to the webhook URL with a retry mechanism.
# Arguments:
#   $1 - The message text to be sent.
#############################
send_webhook_notification() {
  local message="$1"
  
  if [[ -z "$message" ]]; then
    log_message "ERROR" "notification" "No message provided to send_webhook_notification."
    return 1
  fi

  # Build the JSON payload using jq for safety.
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
    # Send the request with curl, capturing both response body and HTTP status.
    local response
    set +e # Do not exit immediately if curl fails.
    if ! response=$(curl -s --fail --max-time "$TIMEOUT" -w "HTTPSTATUS:%{http_code}" \
      -X POST -H 'Content-Type: application/json' -d "$payload" "$WEBHOOK_URL"); then
      log_message "DEBUG" "notification" "Attempt ${attempt}: Network or curl error encountered. Retrying in ${RETRY_DELAY} seconds..."
      sleep "$RETRY_DELAY"
      continue
    fi
    set -e # Exit immediately if curl fails.

    # Extract HTTP status code.
    local http_code
    http_code=$(echo "$response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')
    # Extract the response body.
    local response_body
    response_body=$(echo "$response" | sed -e 's/HTTPSTATUS:.*//')

    if [[ "$http_code" =~ $SUCCESS_CODE_PATTERN ]]; then
      log_message "DEBUG" "notification" "Webhook notification sent successfully on attempt ${attempt} (HTTP $http_code)."
      return 0
    else
      log_message "DEBUG" "notification" "Attempt ${attempt} returned HTTP $http_code with response: $response_body. Retrying in ${RETRY_DELAY} seconds..."
      sleep "$RETRY_DELAY"
    fi
  done

  log_message "WARN" "notification" "Failed to send webhook notification after ${RETRY_COUNT} attempts."
  return 1
}