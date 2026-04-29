# Installation Instructions

This document outlines the steps required to install and configure the Data Encryption Pipeline to ADLS application.

## 1. Clone the Repository

Open your terminal and run:

```bash
git clone https://github.com/your-username/data-encryption-pipeline-to-adls.git
cd data-encryption-pipeline-to-adls
```

## 2. Configure the Environment
Edit the configuration file at config/config.env with your specific settings.\
For example:
```bash
# Directory to monitor for incoming files
MONITOR_DIR="/var/sftp/data"

# Output directory for processed Parquet files
BASE_OUTPUT_DIR="/var/sftp/output"

# Log directory
BASE_LOG_DIR="/var/sftp/log"

# Webhook URL for notifications
WEBHOOK_URL="https://your-webhook-url.com"

# Azure Storage Account name
AZURE_BLOB_STORAGE_ACCOUNT="yourstorageaccount"
```
Also update `config/file_ctrt_col.json` to define processing rules (e.g., table names, columns for encryption, columns to skip luhn check, delimiter).\
For example:
```json
   {
    "table_name": {
        "container": "dss",
        "cc_columns": ["KYF", "OYA_KYF"],
        "free_text_columns": [],
        "skip_check_columns": ["FREE_TEXT_COLUMN", "FREE_TEXT_COL_2"],
        "delimiter": ","
    }
   }
   ```

## 3. Install System Dependencies

1. **Install the required system packages**: inotify-tools, curl, bats-core, and jq are required for running the application.

2. **Install Apache Spark**:
   Follow the instructions on the [Apache Spark website](https://spark.apache.org/downloads.html) to install Spark.\
   One simple way is to download the tar or zip file from the above website to the server where you will run this application.\
   After untar/unzip, rename and move the **spark** folder to **/usr/local/spark** and make sure it is executable by the application and users by changing the owner and group.

3. **Install HashiCorp Vault**:
   Follow the instructions on the [HashiCorp Vault website](https://www.vaultproject.io/docs/install) to install and use Vault and FPE.\
   This application assumes that you already have Vault running and have **url, role_id, and secret_id** ready to call vault for encoding 16 digit numbers.\
   You do require a python client to call vault and this application requires **hvac** client which is defined in the **requirements.txt**.\
   In `config/config.env`, update variable `KMS_CONFIDENTIAL_FPE_PATH` to point to a json file with below information. Defaults to **/etc/vault/kms_confidential_fpe.json**
   ```json
   {
    "url": "https://vault-hostname.com:port",
    "role_id": "xxxxx",
    "secret_id": "xxxxx"
   }
   ```

4. **Install AzCopy**:
   Follow the instructions on the [Microsoft Azure website](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10) to install AzCopy.\
   One simple way is to download the tar or zip file from the above website to the server where you will run this application.\
   After untar/unzip, move the **azcopy** executable to **/usr/local/bin** and make sure it is executable by the application.\
   This application also assumes that Service Principal credentials are provided as environment variables for AzCopy to authenticate with Azure.\
   You may define it a bash file and source it before running the application. For example, you may have a file at **/etc/azcopy_sp_env.sh** which contains details about the **Service Principal** to be used by **AzCopy** to send data to ADLS and other proxy information.\
   Or you can define it in your current shell session.
   ```bash
   #!/bin/bash
   #Exporting AzCopy Service Principal Credentials
   export AZCOPY_AUTO_LOGIN_TYPE=SPN
   export AZCOPY_SPA_APPLICATION_ID=xxxxxxxxxxxxxxxxx
   export AZCOPY_SPA_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxx
   export AZCOPY_TENANT_ID=xxxxxxxxxxxxxxxxxxxxx

   #Exporting HTTP Proxy var for AzCopy Auth with Azure
   export HTTPS_PROXY=http://0.0.0.0:80
   export HTTP_PROXY=http://0.0.0.0:80
   export NO_PROXY=.core.windows.net,.azuredatabricks.net,.vault-kms.card.xxxx.com
   ```
5. **Install Python Dependencies**:\
   Install the python dependencies using pip. In our case, we set the target directory to be **/usr/local/lib/python3.9/site-packages**.\
   This is the default location where python installs packages. This ensures that the application can access the packages without having to set PYTHONPATH.\
   You may change it to be whatever you prefer. 
   ```bash
   pip install --target=/usr/local/lib/python3.9/site-packages -r requirements.txt
   ```

6. **Set Up the Systemd Service**:\
    Copy the service file from the services/ directory to /etc/systemd/system/:
    ```bash
    sudo cp services/file_monitoring.service /etc/systemd/system/
    ```
    Enable and start the service:
    ```bash
    sudo systemctl daemon-reload
    sudo systemctl enable file_monitoring.service
    sudo systemctl start file_monitoring.service
    ```
    Refer to the Usage Instructions for managing the service.