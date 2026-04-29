# Detailed System Pipeline Flow

This document details the flow, key file components, and responsibilities of each module in the Data Encryption Pipeline. 

---

## Table of Contents

1. [Overview](#overview)
2. [File Monitoring & Queue Management](#file-monitoring--queue-management)
   - [file_monitoring.sh](#file_monitoringsh)
   - [locks.sh](#lockssh)
   - [logging.sh](#loggingsh)
   - [notify.sh](#notifysh)
   - [queue_manager.sh](#queue_managersh)
   - [state_manager.sh, temp_tracker.sh, utils.sh, metrics.sh](#others)
3. [File Processing](#file-processing)
   - [file_processing.sh](#file_processingsh)
4. [System Flow Summary](#system-flow-summary)
5. [Locking & Error Handling Strategies](#locking--error-handling-strategies)
6. [Deployment and Testing Considerations](#deployment-and-testing-considerations)

---

## Overview

The Data Encryption Pipeline is designed for secure processing and encryption of sensitive CSV data before transferring the output to Azure Data Lake Storage (ADLS). The system comprises two main parts:

1. **File Monitoring & Queue Management**  
   - Continuously monitors a designated directory.  
   - Maintains a JSON-based processing queue with file-status tracking.  
   - Uses robust locking and error-notification mechanisms.

2. **File Processing**  
   - Processes CSV files by running Spark jobs and related transformation scripts.  
   - Validates inputs, ensures connectivity with Azure, and uploads processed data and trigger files via AzCopy.  
   - Performs cleanup and sends notifications on error or completion.

---

## File Monitoring & Queue Management

### file_monitoring.sh

- **Purpose:**  
  Monitors the configured directory for new CSV (or temporary) files using `inotifywait`. When a new file is detected, it calls functions to add the file to the processing queue or update temporary file trackers.

- **Key Actions:**  
  - Validates that critical environment variables are set (e.g., `BASE_APP_DIR`).  
  - Sources configuration files and shared libraries (`config.env`, `logging.sh`, `notify.sh`, `locks.sh`, etc.).  
  - Make sure to update the `config/config.env` file with the correct values for your environment.
  - Sets global variables like `QUEUE_FILE`, `TEMP_FILE_TRACKER`, and establishes signal traps to call `cleanup_and_exit`.
  - Spawns background processes for directory monitoring, queue processing, metrics logging, and state backup.

### locks.sh

- **Purpose:**  
  Provides file-based locking functions to coordinate access to shared JSON state files (e.g., file queue).

- **Functions:**
  - `acquire_lock`: Tries to create a lock file (with the current PID) and removes stale locks if the PID is not active.
  - `release_lock`: Removes the lock file if the current process owns it.
  - **Error Handling:**  
    Notifies operators via the webhook if the lock cannot be acquired after a set number of attempts.

### logging.sh

- **Purpose:**  
  Centralizes logging with a JSON message format that includes timestamps, log levels, and messages.  
- **Features:**  
  Supports multiple log levels (DEBUG, INFO, WARN, ERROR) and writes to a designated log file.

### notify.sh

- **Purpose:**  
  Sends notifications (such as Microsoft Teams webhooks) when errors occur.  
- **Features:**  
  Formats messages into JSON payloads and implements retry logic.

### queue_manager.sh

- **Purpose:**  
  Manages the JSON-based file queue for processing.

- **Core Functions:**
  - **update_temp_file_tracker:**  
    Adds temporary files (e.g., `.tmp`, `.filepart`) to a separate tracker for later cleanup.
  - **add_to_queue:**  
    Adds a new file entry with status `"pending"` into the queue. Uses `with_lock` to ensure exclusive access.
  - **update_queue_file:**  
    Generic function to update the queue (e.g., marking files as "processing", "processed", or "failed") using jq with locked access. Sends a webhook if an error occurs.
  - **process_queue:**  
    Main loop that:
    - Retrieves a pending file (under lock).
    - Marks it as processing.
    - Launches the processing script (`file_processing.sh`) for that file.
    - Based on exit status, marks the file as either processed or failed and removes it from the queue.
  - **scan_existing_files:**  
    Scans the monitor directory for files not yet in the queue and adds them.
  - **check_deleted_files / check_failed_files / reset_processing_files:**  
    Utilities to maintain queue integrity (removing entries for missing files, notifying on persistent failures, and resetting stuck processing entries).

### Others

- **state_manager.sh:**  
  Manages backups and restoration of the JSON queue and temporary file tracker.
- **temp_tracker.sh:**  
  Specifically tracks temporary files, cleaning up those that are stale.
- **utils.sh:**  
  Provides utility functions, e.g., to initialize a JSON file, cleanup temporary files, and define `cleanup_and_exit` (used in traps).
- **metrics.sh:**  
  Records performance and operational metrics (e.g., processed count, failed count).

---

## File Processing

### file_processing.sh

- **Purpose:**  
  Processes an input CSV file. It validates the file, launches a Spark job for data processing, uploads processed output to ADLS, and performs cleanup.

- **Key Sections and Functions:**
  - **Global Variables & Traps:**  
    - Sets strict mode (set -euo pipefail) and traps signals (SIGINT, SIGTERM, EXIT) to call cleanup.
  - **Environment & Configuration Validation:**  
    - Ensures that `BASE_APP_DIR` and required environment variables (e.g., `AZURE_BLOB_STORAGE_ACCOUNT`, `WEBHOOK_URL`) exist.
  - **Input File Validation:**  
    - `validate_input_file`: Verifies that the input file exists and contains enough data.
    - `validate_json_file`: Checks configuration JSON for validity.
  - **File Information Extraction:**  
    - `extract_file_info`: Parses the file name to obtain `BASE_FILE_NAME`, date parts (year, month), and sets up directories for output and logging.
  - **Azure Connectivity and Marker Checks:**  
    - `test_azure_connectivity`: Uses azcopy to verify connectivity to the target Azure container.
    - `check_existing_marker`: Prevents duplicate processing if data or trigger markers already exist in Azure.
  - **Spark Job Execution:**  
    - `run_spark_job`: Wraps the call to spark-submit with retry logic and measures job duration.
  - **Data Upload and Verification:**  
    - `upload_with_retries`: Uses AzCopy, with exponential backoff, to upload processed data.
    - `verify_upload`: Confirms that the data upload succeeded.
  - **Cleanup:**  
    - `cleanup`: Removes temporary directories and files depending on whether the processing job succeeded or failed.

- **Flow Summary:**  
  1. Validate environment and input file.  
  2. Extract file info and check for duplicate processing markers in Azure.  
  3. Run the Spark job to process the file.  
  4. On successful processing, upload processed data and trigger files to ADLS.  
  5. Verify uploads and perform cleanup.  
  6. Send webhook notifications on any critical errors.

---

## System Flow Summary

1. **File Monitoring Phase**  
   - The `file_monitoring.sh` script kicks off, validating configuration and sourcing required helper libraries.
   - The monitor uses inotify to watch the incoming file directory and, upon detecting new files, calls functions to add files to the queue (via `add_to_queue`) or to update temporary trackers.

2. **Queue Management and Processing Dispatch**  
   - The `process_queue` function within `queue_manager.sh` continuously polls the file queue.  
   - For each pending file, it updates the status to "processing" under a safe locked operation and dispatches the process by invoking `file_processing.sh`.

3. **File Processing Phase**  
   - The `file_processing.sh` script validates inputs, extracts metadata, and runs a Spark job to process the CSV.
   - After processing, the script uses AzCopy to upload results to ADLS and verifies the upload.
   - Once complete, it cleans up local files and directories.

4. **Error Handling and Notifications**  
   - Throughout both file monitoring and processing, any errors are logged with detailed messages.
   - Critical failures send webhook notifications (e.g., via Microsoft Teams) so that operators can intervene.
   - Locking functions and external command failures trigger notifications so that stale locks or temporary failures are visible and, if needed, remedied.

---

## Locking & Error Handling Strategies

- **Locking Mechanism:**  
  - The system uses file-based locking (via `acquire_lock`/`release_lock`) to prevent race conditions when multiple processes access shared JSON files.
  - The `with_lock` wrapper ensures that locks are automatically released, even when errors occur.
  - In case lock acquisition fails repeatedly (or if stale locks are detected), the system sends a webhook notification for operator visibility.

- **Error Notifications:**  
  - Webhook notifications are sent on critical events, such as failures in file processing, queue updates, or lock acquisition.
  - Operators receive these notifications (via Teams, for example) for timely intervention.

---

## Deployment and Testing Considerations

- **Environment Validation:**  
  The scripts validate that all required environment variables and directories (e.g., `BASE_APP_DIR`, `BASE_LOG_DIR`, etc.) are set before execution.

- **Signal Handling:**  
  Signal traps are set to ensure cleanup functions are called upon termination (SIGINT, SIGTERM, EXIT), protecting against stale state and orphaned locks.

- **Integration with Systemd:**  
  The file monitoring script might be deployed as a systemd service to ensure it starts on boot and is properly restarted on failure.

- **Testing:**  
  Use automated tests (Bash tests with bats-core and Python tests with pytest) to validate individual functions and the overall integration.
