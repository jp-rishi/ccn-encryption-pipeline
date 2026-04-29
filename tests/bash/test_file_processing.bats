#!/usr/bin/env bats

setup() {
  # Create a temporary directory for testing.
  export TEST_DIR=$(mktemp -d)
  cd "$TEST_DIR"

  # Create required directories.
  mkdir -p app/scripts app/config output logs temp

  # Use externally set BASE_APP_DIR if available; otherwise, set it.
  : ${BASE_APP_DIR:="$TEST_DIR/app"}

  # Set environment variables required by process_csv.sh.
  export BASE_OUTPUT_DIR="$TEST_DIR/output"
  export BASE_LOG_DIR="$TEST_DIR/logs"
  export BASE_TEMP_DIR="$TEST_DIR/temp"
  export CC_ENCODE_SCRIPT="$TEST_DIR/dummy_cc_encode.sh"
  export MAPPING_SCRIPT="$TEST_DIR/dummy_mapping.sh"
  export SPARK_CONF="--master local"
  export WEBHOOK_URL="http://example.com/webhook"

  # Create dummy logging.sh (simulate log_message function).
  cat << 'EOF' > "$BASE_APP_DIR/scripts/logging.sh"
#!/bin/bash
log_message() {
    echo "[$(date --iso-8601=seconds)] $1: $2 - $3 ($4)"
}
EOF
  chmod +x "$BASE_APP_DIR/scripts/logging.sh"

  # Create dummy notify.sh (simulate send_webhook_notification).
  cat << 'EOF' > "$BASE_APP_DIR/scripts/notify.sh"
#!/bin/bash
send_webhook_notification() {
    echo "Webhook notification sent: $1"
}
EOF
  chmod +x "$BASE_APP_DIR/scripts/notify.sh"

  # Create dummy config file.
  cat << EOF > "$BASE_APP_DIR/config/config.env"
AZURE_BLOB_STORAGE_ACCOUNT="testaccount"
EOF

  # Create dummy file_ctrt_col.json mapping base table "t_ds00020d_l00" to container "dss".
  cat << EOF > "$BASE_APP_DIR/config/file_ctrt_col.json"
{
  "t_ds00020d_l00": {
    "container": "dss"
    "delimiter": ","
  }
}
EOF
  export INPUT_FILE_CONFIG="$BASE_APP_DIR/config/file_ctrt_col.json"

  # Create dummy stubs for azcopy and spark-submit.
  mkdir -p "$TEST_DIR/test_bin"

  # Dummy azcopy stub: outputs simulated JSON when invoked with "list".
  cat << 'EOF' > "$TEST_DIR/test_bin/azcopy"
#!/bin/bash
if [[ "$*" == *"list"* ]]; then
    echo '{"TimeStamp":"2025-02-20T12:00:00+00:00","MessageType":"Info","MessageContent":"INFO: Proxy detected: dummy proxy"}'
    echo '{"TimeStamp":"2025-02-20T12:00:01+00:00","MessageType":"Info","MessageContent":"INFO: SPN Auth via secret succeeded."}'
    echo '{"TimeStamp":"2025-02-20T12:00:02+00:00","MessageType":"Info","MessageContent":"INFO: Authenticating to source using Azure AD"}'
    echo '{"TimeStamp":"2025-02-20T12:00:03+00:00","MessageType":"EndOfJob","MessageContent":""}'
    exit 0
else
    exit 0
fi
EOF
  chmod +x "$TEST_DIR/test_bin/azcopy"

  # Dummy spark-submit stub: creates _SUCCESS file in OUTPUT_FILE_DIR.
  cat << 'EOF' > "$TEST_DIR/test_bin/spark-submit"
#!/bin/bash
if [[ -n "$OUTPUT_FILE_DIR" ]]; then
    mkdir -p "$OUTPUT_FILE_DIR"
    touch "$OUTPUT_FILE_DIR/_SUCCESS"
fi
exit 0
EOF
  chmod +x "$TEST_DIR/test_bin/spark-submit"

  # Create dummy CC_ENCODE_SCRIPT and MAPPING_SCRIPT.
  cat << 'EOF' > "$TEST_DIR/dummy_cc_encode.sh"
#!/bin/bash
echo "Running dummy CC_ENCODE_SCRIPT"
EOF
  chmod +x "$TEST_DIR/dummy_cc_encode.sh"
  
  cat << 'EOF' > "$TEST_DIR/dummy_mapping.sh"
#!/bin/bash
echo "Running dummy MAPPING_SCRIPT"
EOF
  chmod +x "$TEST_DIR/dummy_mapping.sh"

  # Create a dummy CSV input file named t_ds00020d_l00_20240930.csv.
  cat << EOF > t_ds00020d_l00_20240930.csv
header1,header2
value1,value2
EOF

  # Adjust PATH to include our test_bin stubs.
  export PATH="$TEST_DIR/test_bin:$PATH"

  # Copy process_csv.sh from your production BASE_APP_DIR/scripts directory.
  CP_TARGET="$BASE_APP_DIR/scripts/process_csv.sh"
  if [ ! -f "$CP_TARGET" ]; then
      skip "process_csv.sh not found at $CP_TARGET"
  else
      cp "$CP_TARGET" .
  fi
}

teardown() {
  cd /
  rm -rf "$TEST_DIR"
}

@test "process_csv runs successfully with dummy input" {
  run bash process_csv.sh t_ds00020d_l00_20240930.csv
  [ "$status" -eq 0 ]
  # Check that _SUCCESS file exists in the output directory.
  [ -f "$BASE_OUTPUT_DIR/$(basename t_ds00020d_l00_20240930.csv .csv)/_SUCCESS" ]
}

@test "process_csv fails if input file is missing" {
  run bash process_csv.sh nonexisting.csv
  [ "$status" -ne 0 ]
}

@test "process_csv aborts when markers exist" {
  # Create a dummy marker for data.
  mkdir -p "$BASE_OUTPUT_DIR/t_ds00020d_l00/t_ds00020d_l00/2024/09/t_ds00020d_l00_20240930"
  touch "$BASE_OUTPUT_DIR/t_ds00020d_l00/t_ds00020d_l00/2024/09/t_ds00020d_l00_20240930/_SUCCESS"
  run bash process_csv.sh t_ds00020d_l00_20240930.csv
  [ "$status" -ne 0 ]
}
