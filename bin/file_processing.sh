#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

################################################################################
#
# This script processes an input CSV file by validating input,
# invoking Spark for data processing, and then uploading output data to Azure.
# It exits with a nonzero status if any critical step (like data upload) fails.
# Trigger file upload/verification failures are treated as nonfatal.
#
################################################################################

#########################
# Global Variables      
#########################
job_success=false
# Pre-declare globals so cleanup doesn’t fail if they aren’t set.
BASE_FILE_NAME=""
TEMP_FILE_DIR=""
OUTPUT_FILE_DIR=""
INPUT_FILE_CONFIG=""

# Trap SIGINT/SIGTERM to perform cleanup.
trap 'echo "Interrupt or termination signal received. Cleaning up..."; cleanup; exit 1' SIGINT SIGTERM
# Also trap EXIT so that cleanup is always called.
trap 'cleanup' EXIT

# Ensure BASE_APP_DIR is set.
if [ -z "${BASE_APP_DIR:-}" ]; then
  echo "Error: BASE_APP_DIR is not set. Please set it before executing this script."
  exit 1
fi

# Source configuration.
source "$BASE_APP_DIR/config/config.env" || { echo "Failed to source config.env"; exit 1; }

# Override LOG_FILE for monitoring.
LOG_FILE="$BASE_LOG_DIR/file_processing.log"

# Source common functions.
source "$BASE_APP_DIR/bin/includes/logging.sh" || { echo "Failed to source logging.sh"; exit 1; }
source "$BASE_APP_DIR/bin/includes/notify.sh" || { log_message "ERROR" "file_processing" "Failed to source notify.sh"; exit 1; }

###########################################
# VALIDATE REQUIRED ENVIRONMENT VARIABLES
###########################################
validate_env_vars() {
  local required_vars=("AZURE_BLOB_STORAGE_ACCOUNT" "AZCOPY_SP_CREDENTIALS" "BASE_OUTPUT_DIR" "BASE_LOG_DIR" "BASE_TEMP_DIR" "CC_ENCODE_SCRIPT" "INPUT_FILE_CONFIG" "MAPPING_SCRIPT" "SPARK_CONF" "WEBHOOK_URL")
  for var in "${required_vars[@]}"; do
    if [[ -z "${!var:-}" ]]; then
      log_message "ERROR" "file_processing" "Required environment variable $var is not set."
      send_webhook_notification "Process CSV failed: required env var $var is not set."
      exit 1
    fi
  done
}
validate_env_vars

# Source AzCopy credentials.
source "$AZCOPY_SP_CREDENTIALS" || { 
    log_message "ERROR" "file_processing" "Failed to source azcopy_sp_env.sh"; 
    send_webhook_notification "Process CSV failed: could not source azcopy_sp_env.sh."
    exit 1; 
}

# Input: Must supply an input file.
if [[ $# -lt 1 || -z "$1" ]]; then
  log_message "ERROR" "file_processing" "Usage: $0 <input_file>"
  send_webhook_notification "Process CSV failed: no input file provided."
  exit 1
fi
INPUT_FILE="$1"

#############################
# FILE VALIDATION FUNCTIONS
#############################
validate_input_file() {
    local file_path="$1"
    if [[ ! -f "$file_path" ]]; then
        log_message "ERROR" "file_processing" "Input file does not exist: $file_path"
        send_webhook_notification "Process CSV failed: input file $file_path not found."
        exit 1
    fi
    if [[ $(head -n 2 "$file_path" | wc -l) -le 1 ]]; then
        log_message "ERROR" "file_processing" "$file_path contains only a single line and cannot be processed." "$file_path" ""
        rm "$file_path"
        send_webhook_notification "Process CSV failed: input file $file_path contains only one line."
        exit 1
    fi
}

validate_json_file() {
    local file_path="$1"
    local descriptor="$2"
    if [[ ! -f "$file_path" ]] || ! jq . "$file_path" &>/dev/null; then
        log_message "ERROR" "file_processing" "$descriptor file is invalid or does not exist: $file_path"
        send_webhook_notification "Process CSV failed: $descriptor file $file_path invalid or missing."
        exit 1
    fi
}

################################
# FILE INFO EXTRACTION FUNCTION
################################
extract_file_info() {
  BASE_FILE_NAME=$(basename "$INPUT_FILE" .csv)
  if [[ "$BASE_FILE_NAME" == "contract_uid_mapping_table" ]]; then
      base_table_name="$BASE_FILE_NAME"
      date_part=""
      year=""
      month=""
      log_message "DEBUG" "file_processing" "File identified as contract_uid_mapping_table; skipping date extraction." "$INPUT_FILE" ""
  else
      base_table_name=$(echo "$BASE_FILE_NAME" | sed -E 's/_[0-9]+_[0-9]{8}$//; s/_[0-9]{8}$//')
      log_message "INFO" "file_processing" "Extracted base table name: $base_table_name" "$INPUT_FILE" ""
      date_part=$(echo "$BASE_FILE_NAME" | grep -oE '[0-9]{8}')
      if [[ -z "$date_part" ]]; then
          log_message "ERROR" "file_processing" "Date part not found in file name: $INPUT_FILE"
          send_webhook_notification "Process CSV failed: date part not found in $INPUT_FILE."
          exit 1
      fi
      year=${date_part:0:4}
      month=${date_part:4:2}
  fi

  # Set AzCopy log locations under BASE_LOG_DIR/BASE_FILE_NAME/azcopy/ and create directory.
  export AZCOPY_JOB_PLAN_LOCATION="$BASE_LOG_DIR/${BASE_FILE_NAME}/azcopy/"
  export AZCOPY_LOG_LOCATION="$BASE_LOG_DIR/${BASE_FILE_NAME}/azcopy/"
  mkdir -p "$AZCOPY_LOG_LOCATION"
}

################################
# TEST AZURE CONNECTIVITY FUNCTION
################################
test_azure_connectivity() {
    local container_name="$1"
    local container_url="https://$AZURE_BLOB_STORAGE_ACCOUNT.blob.core.windows.net/$container_name"
    log_message "DEBUG" "file_processing" "Testing Azure connectivity to container: $container_url" ""
    set +e
    local connectivity_output
    connectivity_output=$(head -n 10 < <(azcopy list "$container_url" --output-type json 2>/dev/null))
    local exit_status=$?
    set -e
    if [[ -z "$connectivity_output" ]]; then
         log_message "ERROR" "file_processing" "Azure connectivity test failed: no output from azcopy list for $container_url"
         send_webhook_notification "Process CSV failed: no output from azcopy list for container $container_url."
         return 1
    fi
    if echo "$connectivity_output" | grep -q '"MessageType":"ListObject"' || \
       echo "$connectivity_output" | grep -q '"MessageType":"EndOfJob"'; then
         log_message "DEBUG" "file_processing" "Azure connectivity test succeeded for container: $container_url"
         return 0
    else
         log_message "ERROR" "file_processing" "Azure connectivity test failed: expected messages not found. Output: $connectivity_output"
         send_webhook_notification "Process CSV failed: Azure connectivity test did not return expected messages for container $container_url."
         return 1
    fi
}

################################
# CHECK EXISTING MARKER FUNCTION
################################
check_existing_marker() {
    local target_url="$1"
    set +e
    local marker_output
    marker_output=$(head -n 10 < <(azcopy list "$target_url" --output-type json 2>/dev/null) | tail -n +4)
    set -e
    if [[ -n "$marker_output" && $(echo "$marker_output" | grep -q '"MessageType":"ListObject"'; echo $?) -eq 0 ]]; then
        return 0  # Marker exists
    else
        return 1  # No marker found
    fi
}

############################
# SPARK JOB RETRY FUNCTION
############################
run_spark_job() {
    local script="$1"
    shift
    local args=("$@")
    local retries=2
    local initial_sleep=60
    local sleep_time="$initial_sleep"
    local oldIFS="$IFS"
    IFS=$' \t\n'
    local spark_conf_arr=($SPARK_CONF)
    IFS="$oldIFS"
    for ((attempt = 1; attempt <= retries; attempt++)); do
        log_message "INFO" "file_processing" "Running Spark job (Attempt $attempt of $retries)" "$INPUT_FILE" ""
        start_time=$(date +%s%3N)
        set +e
        spark-submit "${spark_conf_arr[@]}" "$script" "${args[@]}"
        spark_status=$?
        set -e
        end_time=$(date +%s%3N)
        duration=$((end_time - start_time))
        if [[ $spark_status -eq 0 && -f "$OUTPUT_FILE_DIR/_SUCCESS" ]]; then
            log_message "INFO" "file_processing" "Spark job completed successfully for $INPUT_FILE (Attempt $attempt). _SUCCESS file found." "$INPUT_FILE" "${duration}ms"
            return 0
        else
            if [[ $spark_status -ge 128 ]]; then
                signal=$((spark_status - 128))
                log_message "ERROR" "file_processing" "Spark job killed by signal $signal for $INPUT_FILE (Attempt $attempt)" "$INPUT_FILE" "${duration}ms"
            else
                log_message "ERROR" "file_processing" "Spark job failed for $INPUT_FILE with exit status $spark_status (Attempt $attempt)" "$INPUT_FILE" "${duration}ms"
            fi
            if [[ $attempt -eq $retries ]]; then
                log_message "ERROR" "file_processing" "Spark job failed after $retries attempts for $INPUT_FILE" "$INPUT_FILE" ""
                send_webhook_notification "Process CSV failed: Spark job failed after $retries attempts for $INPUT_FILE."
                cleanup
                log_message "DEBUG" "file_processing" "Exiting after Spark job failure" "$INPUT_FILE" ""
                exit 1
            fi
            log_message "DEBUG" "file_processing" "Sleeping for $sleep_time seconds before next Spark job attempt." "$INPUT_FILE" ""
            sleep "$sleep_time"
            sleep_time=$(( sleep_time * 2 ))
        fi
    done
}

################################
# VERIFY UPLOAD FUNCTION
################################
verify_upload() {
    local source_path="$1"
    local destination_path="$2"
    local status=0
    local expected_dest=""
    if [[ "$source_path" == *.trigger ]]; then
         expected_dest="${destination_path%/}/$(basename "$source_path")"
    elif [[ -d "$source_path" ]]; then
         expected_dest="${destination_path%/}/$(basename "$source_path")"
    else
         expected_dest="$destination_path"
    fi
    if [[ "$source_path" == *.trigger ]]; then
        log_message "DEBUG" "file_processing" "Using simplified verification for trigger file: $source_path" "$source_path" ""
        set +e
        local list_output
        list_output=$(azcopy list "$expected_dest" --output-type json 2>/dev/null | tail -n +4 | head -n 10)
        set -e
        if echo "$list_output" | grep -q '"MessageType":"ListObject"'; then
            log_message "DEBUG" "file_processing" "Trigger file verification succeeded: $expected_dest" "$source_path" ""
        else
            log_message "ERROR" "file_processing" "Trigger file verification failed: no ListObject messages. Output: $list_output" "$source_path" ""
            status=1
        fi
    else
        log_message "DEBUG" "file_processing" "Verifying upload with azcopy sync for $source_path against $expected_dest" "$source_path" ""
        set +e
        local sync_output
        sync_output=$(azcopy sync "$source_path" "$expected_dest" --recursive=true --exclude-pattern="*.crc;_SUCCESS" --delete-destination=false --compare-hash=MD5 2>&1)
        local sync_status=$?
        set -e
        if [[ $sync_status -ne 0 ]]; then
            log_message "ERROR" "file_processing" "Azcopy sync failed with status $sync_status. Output: $sync_output" "$source_path" ""
            status=$sync_status
        else
            status=0
        fi
    fi
    return "$status"
}

################################
# AZCOPY UPLOAD RETRY FUNCTION
################################
upload_with_retries() {
    local source_path="$1"
    local destination_path="$2"
    local retries=2
    local initial_sleep=60
    local sleep_time="$initial_sleep"
    for ((attempt = 1; attempt <= retries; attempt++)); do
        log_message "INFO" "file_processing" "Uploading $source_path to $destination_path (Attempt $attempt of $retries)" "$source_path" ""
        set +e
        azcopy copy "$source_path" "$destination_path" --recursive=true --exclude-pattern="*.crc;_SUCCESS" --overwrite=false --put-md5
        upload_status=$?
        set -e
        if [[ $upload_status -eq 0 ]]; then
            log_message "DEBUG" "file_processing" "Upload copy successful for $source_path" "$source_path" ""
            verify_upload "$source_path" "$destination_path"
            if [[ $? -eq 0 ]]; then
                log_message "DEBUG" "file_processing" "Upload verification succeeded for $source_path" "$source_path" ""
                return 0
            else
                log_message "ERROR" "file_processing" "Verification failed for $source_path on attempt $attempt" "$source_path" ""
            fi
        else
            log_message "ERROR" "file_processing" "Upload copy failed on attempt $attempt for $source_path with status $upload_status" "$source_path" ""
        fi
        if [[ $attempt -eq $retries ]]; then
            log_message "ERROR" "file_processing" "Upload failed after $retries attempts for $source_path" "$source_path" ""
            return 1
        fi
        log_message "DEBUG" "file_processing" "Sleeping for $sleep_time seconds before next upload attempt." "$source_path" ""
        sleep "$sleep_time"
        sleep_time=$(( sleep_time * 2 ))
    done
}

####################
# CLEANUP FUNCTION
####################
cleanup() {
    set +e
    if [[ -n "$TEMP_FILE_DIR" && -d "$TEMP_FILE_DIR" ]]; then
        rm -rf "$TEMP_FILE_DIR"
        log_message "DEBUG" "file_processing" "Temporary directory cleaned up: $TEMP_FILE_DIR"
    fi
    if [[ "$job_success" == true ]]; then
        if [[ "$BASE_FILE_NAME" != "contract_uid_mapping_table" ]]; then
            if [[ -f "$OUTPUT_FILE_DIR.trigger" ]]; then
                rm "$OUTPUT_FILE_DIR.trigger"
                log_message "DEBUG" "file_processing" "Trigger file cleaned up: $OUTPUT_FILE_DIR.trigger"
            fi
            if [[ -d "$OUTPUT_FILE_DIR" ]]; then
                rm -rf "$OUTPUT_FILE_DIR"
                log_message "DEBUG" "file_processing" "Output directory cleaned up: $OUTPUT_FILE_DIR"
            fi
        else
            log_message "WARN" "file_processing" "Retaining output directory for $BASE_FILE_NAME: $OUTPUT_FILE_DIR"
        fi
        if [[ -f "$INPUT_FILE" ]]; then
            rm "$INPUT_FILE"
            log_message "DEBUG" "file_processing" "Input file cleaned up: $INPUT_FILE"
        fi
    else
        log_message "WARN" "file_processing" "Job failed. Retaining input file and output directory for debugging."
    fi
    set -e
}

#################
# MAIN FUNCTION
#################
main() {
    validate_input_file "$INPUT_FILE"
    validate_json_file "$INPUT_FILE_CONFIG" "Input Config"
    log_message "INFO" "file_processing" "Processing:" "$INPUT_FILE"
    extract_file_info

    # Pre-run idempotency checks (for non-contract_uid_mapping_table files)
    if [[ "$BASE_FILE_NAME" != "contract_uid_mapping_table" ]]; then
        container=$(jq -r --arg base_table_name "$base_table_name" '.[$base_table_name].container' "$INPUT_FILE_CONFIG")
        if [[ -z "$container" || "$container" == "null" ]]; then
            log_message "ERROR" "file_processing" "Container not found for base table name: $base_table_name" "$INPUT_FILE"
            send_webhook_notification "Process CSV failed: container not found for base table $base_table_name."
            cleanup
            log_message "DEBUG" "file_processing" "Exiting due to missing container"
            exit 1
        fi
        if ! test_azure_connectivity "$container"; then
            log_message "ERROR" "file_processing" "Azure connectivity test failed for container: $container" "$INPUT_FILE"
            send_webhook_notification "Process CSV failed: Azure connectivity test failed for container $container."
            exit 1
        fi
        # Marker paths include BASE_FILE_NAME.
        azure_marker="https://$AZURE_BLOB_STORAGE_ACCOUNT.blob.core.windows.net/$container/$base_table_name/$year/$month/$BASE_FILE_NAME/"
        if check_existing_marker "$azure_marker"; then
            log_message "ERROR" "file_processing" "Data already exists at $azure_marker. Aborting reprocessing to avoid duplicates." "$INPUT_FILE"
            send_webhook_notification "Data already exists at $azure_marker for input $INPUT_FILE. Please verify manually."
            exit 1
        fi
        trigger_marker="https://$AZURE_BLOB_STORAGE_ACCOUNT.blob.core.windows.net/trigger/$base_table_name/$BASE_FILE_NAME.trigger"
        if check_existing_marker "$trigger_marker"; then
            log_message "ERROR" "file_processing" "Trigger file already exists at $trigger_marker. Aborting reprocessing to avoid duplicates." "$INPUT_FILE"
            send_webhook_notification "Trigger file already exists at $trigger_marker for input $INPUT_FILE. Please verify manually."
            exit 1
        fi
    fi

    OUTPUT_FILE_DIR="$BASE_OUTPUT_DIR/$BASE_FILE_NAME"
    TEMP_FILE_DIR=$(mktemp -d -p "$BASE_TEMP_DIR" "${BASE_FILE_NAME}.XXXXXX")

    if [[ "$BASE_FILE_NAME" == "contract_uid_mapping_table" ]]; then
        run_spark_job "$MAPPING_SCRIPT" "$INPUT_FILE"
    else
        run_spark_job "$CC_ENCODE_SCRIPT" "$INPUT_FILE" "$INPUT_FILE_CONFIG"
    fi

    if [[ ! "$(ls -A "$OUTPUT_FILE_DIR")" ]]; then
        log_message "ERROR" "file_processing" "Output directory is empty: $OUTPUT_FILE_DIR. Skipping upload." "$INPUT_FILE"
        send_webhook_notification "Process CSV failed: output directory $OUTPUT_FILE_DIR is empty for input $INPUT_FILE."
        cleanup
        log_message "DEBUG" "file_processing" "Exiting due to empty output directory" "$INPUT_FILE"
        exit 1
    fi

    if [[ "$BASE_FILE_NAME" == "contract_uid_mapping_table" ]]; then
        log_message "DEBUG" "file_processing" "Skipping upload for $BASE_FILE_NAME. Output will not be sent to Azure." "$INPUT_FILE"
        job_success=true
        if [[ -f "$INPUT_FILE" ]]; then
            rm "$INPUT_FILE"
            log_message "DEBUG" "file_processing" "Input file cleaned up: $INPUT_FILE"
        fi
        cleanup
        exit 0
    fi

    if [[ -z "$base_table_name" || -z "$year" || -z "$month" ]]; then
        log_message "ERROR" "file_processing" "Invalid Azure path components: base_table_name=$base_table_name, year=$year, month=$month"
        send_webhook_notification "Process CSV failed: invalid Azure path components for $INPUT_FILE."
        cleanup
        log_message "DEBUG" "file_processing" "Exiting due to invalid Azure path components"
        exit 1
    fi

    azure_path="https://$AZURE_BLOB_STORAGE_ACCOUNT.blob.core.windows.net/$container/$base_table_name/$year/$month/"

    # --- Upload output data (fatal if copy fails) ---
    if upload_with_retries "$OUTPUT_FILE_DIR" "$azure_path"; then
        log_message "INFO" "file_processing" "Data upload succeeded for $OUTPUT_FILE_DIR"
    else
        send_webhook_notification "Process CSV failed: data upload failed for $OUTPUT_FILE_DIR."
        cleanup
        log_message "DEBUG" "file_processing" "Exiting due to data upload failure"
        exit 1
    fi

    # --- Verify output data upload (nonfatal) ---
    if verify_upload "$OUTPUT_FILE_DIR" "$azure_path"; then
        log_message "INFO" "file_processing" "Data upload verification succeeded for $OUTPUT_FILE_DIR"
    else
        log_message "WARN" "file_processing" "Data upload verification failed for $OUTPUT_FILE_DIR. Please verify manually."
        send_webhook_notification "Data upload verification failed for base table $base_table_name. Please review manually."
        # Nonfatal: proceed.
    fi

    # --- Upload trigger file (nonfatal) ---
    touch "$OUTPUT_FILE_DIR.trigger"
    trigger_path="https://$AZURE_BLOB_STORAGE_ACCOUNT.blob.core.windows.net/trigger/$base_table_name/"
    if upload_with_retries "$OUTPUT_FILE_DIR.trigger" "$trigger_path"; then
        log_message "INFO" "file_processing" "Trigger file upload succeeded for $OUTPUT_FILE_DIR.trigger"
    else
        log_message "WARN" "file_processing" "Trigger file upload failed for $OUTPUT_FILE_DIR.trigger. Please verify manually."
        send_webhook_notification "Trigger file upload failed for base table $base_table_name. Data upload succeeded. Please review manually."
        # Nonfatal: proceed.
    fi

    # --- Verify trigger file upload (nonfatal) ---
    if verify_upload "$OUTPUT_FILE_DIR.trigger" "$trigger_path"; then
        log_message "INFO" "file_processing" "Trigger file verification succeeded for $OUTPUT_FILE_DIR.trigger"
    else
        log_message "WARN" "file_processing" "Trigger file verification failed for $OUTPUT_FILE_DIR.trigger. Please verify manually."
        send_webhook_notification "Trigger file verification failed for base table $base_table_name. Please review manually."
        # Nonfatal.
    fi

    job_success=true
    cleanup
    exit 0
}

main
