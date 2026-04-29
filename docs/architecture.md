# System Architecture

This document describes the overall architecture and key components of the Data Encryption Pipeline to ADLS.
![Pipeline Diagram](https://github.com/user-attachments/assets/a1e1baf1-d953-40e7-b875-809c18959dde)

## High-Level Overview

The application is divided into several layers, each responsible for a specific part of the workflow:

1. **File Monitoring**  
   - Continuously monitors a specified directory for new files.
   - Uses various helper modules (e.g., file locking, queue management, state management) to manage file ingestion and health checks.
   - Implemented in the `bin/file_monitoring.sh` script and its supporting modules in `bin/includes/`.

2. **Data Processing**  
   - Processes queued files by running pyspark script which detects credit card numbers, encrypts them using HashiCorp Vault's Format-Preserving Encryption (FPE), and writes the output as parquet files.
   - Managed by `bin/file_processing.sh` and Python scripts (e.g., `python/cc_encode.py`).

3. **Data Transfer**  
   - Transfers the processed and encrypted files to Azure Data Lake Storage (ADLS) using AzCopy.
   - Ensures secure and reliable file transfer.
   - Implemented in the `bin/file_processing.sh` script.

## Key Components

- **Shell Scripts (Bash):**
  - `file_monitoring.sh`: Main monitoring script.
  - `file_processing.sh`: Main processing script.
  - `/bin/includes/`: Contains shared helper modules such as:
    - `locks.sh`: File locking functions.
    - `logging.sh`: Logging functions.
    - `metrics.sh`: Performance metrics functions.
    - `notify.sh`: Notification functions.
    - `queue_manager.sh`: Manages file queues.
    - `state_manager.sh`: Handles state backups and health-checks.
    - `temp_tracker.sh`: Tracks temporary files.
    - `utils.sh`: General utility functions.

- **Python Scripts:**
  - `cc_encode.py`: Encrypts credit card numbers using HashiCorp Vault.
  - `mapping_csv_to_parquet_incremental.py`: Processes CSV files and converts them to Parquet.
  - `logging_utils.py`: Provides Python-based logging utilities.

- **Configuration Files:**
  - `config/config.env`: Contains environment-specific variables.
  - `config/file_ctrt_col.json`: Specifies processing rules for columns.

- **Systemd Service:**
  - `services/file_monitoring.service`: Defines the service that runs the file monitoring script.

- **Logging:**
  - Logs are written to various files under `BASE_LOG_DIR` mentioned in `config/config.env` to capture runtime events, errors, and performance metrics.

- **Testing:**
  - Tests are separated into Bash tests (using bats-core) and Python tests (using pytest) and reside in the `tests/` directory.

## Data Flow Diagram

A high-level diagram of the pipeline:

1. New CSV files are dropped or transferred into a monitored directory.
2. `file_monitoring.sh` detects the new files and adds them to a processing queue and runs `file_processing.sh` for each file in the queue.
3. `file_processing.sh` 
    -   calls Python scripts to encrypt CCNs, and converts data to Parquet.
    -   Processed files are transferred to ADLS via AzCopy.
4. Throughout this process, logging and notifications are used to track events and errors.