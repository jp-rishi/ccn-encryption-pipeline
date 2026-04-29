# Credit Card Number Encryption Pipeline

![Pipeline Diagram](https://github.com/user-attachments/assets/a1e1baf1-d953-40e7-b875-809c18959dde)


## Overview
This project involves encrypting and migrating data from on-premises servers to **Azure Data Lake Storage (ADLS)**. The primary focus is on detecting and encrypting **credit card numbers (CCNs)** within the data files before they are transferred to ADLS. The workflow includes:

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

For detailed instructions on installation, usage, system architecture, and testing, please see the documentation in the [docs/](docs/) folder.

## Key Features
- **Automated File Monitoring**: Shell scripts to monitor directories and queue new files for processing.
- **Resilient Queue Management**: Queue operations are resilient to failures and can be restarted without data loss.
- **Credit Card Number Encryption**: Integration with **HashiCorp Vault** to securely encrypt credit card numbers using FPE.
- **Data Processing with Spark**: Efficient data processing and transformation using **Apache Spark**.
- **Parquet Conversion**: Conversion of processed data to **Parquet** format for optimized storage.
- **Secure Data Transfer**: Reliable and secure transfer of data to ADLS using **AzCopy**.

## Project Structure

```plaintext
data-encryption-pipeline-to-adls/
├── bin/                          # Shell scripts (monitoring & processing) and helper modules (includes/)
├── config/                       # Environment variables and processing rules
├── docs/                         # Detailed documentation (Installation, Usage, Architecture, Testing)
├── python/                       # PySpark processing scripts (e.g., cc_encode.py)
├── services/                     # Systemd service files (e.g., file_monitoring.service)
├── tests/                        # Test suites for Bash (bats-core) and Python (pytest)
├── .gitignore
├── requirements.txt              # Python package dependencies
├── README.md                     # This file.
└── LICENSE                       # License
```

## Prerequisites
- Apache Spark
- HashiCorp Vault
- AzCopy
- Required Python packages (see requirements.txt)
- SFTP server (if required for file transfer)

## Documentation
- Installation Instructions
- Usage Instructions
- System Architecture
- Testing Guidelines

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License
