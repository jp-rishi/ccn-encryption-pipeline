# Usage Instructions

This document provides an overview of how to run and interact with the Data Encryption Pipeline.

## Services Overview

The application consists primarily of two shell scripts:

- `file_monitoring.sh`: Monitors a directory for existing and incoming files and adds them to a queue.
- `file_processing.sh`: Processes files by encrypting CCNs using pyspark script, converting data to Parquet, and transferring the files to ADLS.

The `file_monitoring.sh` is intended to run continuously as a systemd service.

## Managing the Service

After installing the service (see [Installation](docs/installation.md)), you can manage the service using systemd commands:

- **Check service status:**

    ```bash
    sudo systemctl status file_monitoring.service
    ```

- **Restart the service (after making changes):**

    ```bash
    sudo systemctl daemon-reload
    sudo systemctl restart file_monitoring.service
    ```

- **View service logs:**

    The service writes runtime logs to the log files under `BASE_LOG_DIR` mentioned in `config/config.env`.

## Running Scripts Directly

For troubleshooting or running scripts interactively:

- **File Monitoring Script** (Bash):

    ```bash
    ./bin/file_monitoring.sh
    ```

- **File Processing Script** (Bash):

    ```bash
    ./bin/file_processing.sh input_file_path
    ```

## Python Components

Python scripts are used for data processing tasks. For example:

- `python/cc_encode.py`: Detects and encrypts credit card numbers using HashiCorp Vault.
   Example usage:
   ```bash
   python3 python/cc_encode.py input_file_path config/file_ctrt_col.json
   ```
- `python/mapping_csv_to_parquet_incremental.py`: Converts contract_uid_mapping_table csv files into Parquet format.
   Example usage:
   ```bash
   python3 python/mapping_csv_to_parquet_incremental.py input_file_path
   ```

You can run these scripts directly if needed, though they are typically called through the `file_processing.sh` shell script.

## Notifications and Alerts

The application sends notifications using the helper module `bin/includes/notify.sh`. Ensure that `WEBHOOK_URL` is correctly configured in your `config/config.env` to receive notifications.
It uses the `curl` command to send a POST request to the configured webhook URL.