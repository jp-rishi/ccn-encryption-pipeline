#!/usr/bin/env bats

setup() {
  tmp_dir=$(mktemp -d)
  # Export necessary environment variables for integration testing.
  export BASE_APP_DIR="$PWD"
  export BASE_LOG_DIR="$tmp_dir/log"
  export BASE_TEMP_DIR="$tmp_dir/temp"
  export BASE_OUTPUT_DIR="$tmp_dir/output"
  export AZURE_BLOB_STORAGE_ACCOUNT="dummyaccount"
  # Dummy AzCopy credentials file.
  export AZCOPY_SP_CREDENTIALS="$PWD/dummy_azcopy.sh"
  export CC_ENCODE_SCRIPT="$PWD/dummy_encode.sh"
  export MAPPING_SCRIPT="$PWD/dummy_mapping.sh"
  export SPARK_CONF="--master local[1]"
  export INPUT_FILE_CONFIG="$tmp_dir/input_config.json"
  export WEBHOOK_URL="http://dummyurl"

  mkdir -p "$BASE_LOG_DIR" "$tmp_dir/temp" "$tmp_dir/output" "$tmp_dir/data"
  # Create a dummy config JSON for input config.
  echo '{"dummy_table": {"container": "dummycontainer"}}' > "$INPUT_FILE_CONFIG"

  # Create dummy versions of external commands:
  # Dummy azcopy script (always returns success).
  echo '#!/bin/bash' > dummy_azcopy.sh
  echo 'exit 0' >> dummy_azcopy.sh
  chmod +x dummy_azcopy.sh

  # Dummy spark-submit in the encoding script:
  # It will simply touch a _SUCCESS file in the output directory.
  echo '#!/bin/bash' > dummy_encode.sh
  echo 'if [ "$#" -ge 2 ]; then touch "$2/_SUCCESS"; fi; exit 0' >> dummy_encode.sh
  chmod +x dummy_encode.sh

  # Dummy mapping script – simply exit 0.
  echo '#!/bin/bash; exit 0' > dummy_mapping.sh
  chmod +x dummy_mapping.sh

  # Create a dummy input JSON file for test purposes.
  # (This is used by file_processing.sh for configuration.)
  echo '{}' > "$tmp_dir/dummy_input_config.json"
  
  # Export QUEUE_FILE used by file_processing.sh (if needed)
  export QUEUE_FILE="$tmp_dir/file_queue.json"
  echo "[]" > "$QUEUE_FILE"
}

teardown() {
  rm -rf "$tmp_dir"
  rm -f dummy_azcopy.sh dummy_encode.sh dummy_mapping.sh
}

####################################
# Integration Test: file_processing.sh failure for missing input file.
####################################
@test "file_processing.sh fails when no input file is provided" {
  run "$BASE_APP_DIR/file_processing.sh"
  [ "$status" -ne 0 ] || fail "file_processing.sh should fail if input file is missing."
}

####################################
# Integration Test: file_processing.sh processes a valid input file (non-contract_uid_mapping_table)
####################################
@test "file_processing.sh processes a valid input file" {
  # Create a dummy CSV input file with more than one line.
  input_file="$tmp_dir/test_valid.csv"
  echo "header" > "$input_file"
  echo "data" >> "$input_file"
  
  # For non contract_uid_mapping_table files, the extract_file_info function
  # expects a date portion in the filename. We include an 8-digit date.
  mv "$input_file" "$tmp_dir/test_valid_20250101.csv"
  input_file="$tmp_dir/test_valid_20250101.csv"
  
  # Run file_processing.sh.
  run "$BASE_APP_DIR/file_processing.sh" "$input_file"
  [ "$status" -eq 0 ] || fail "file_processing.sh did not succeed with a valid input."
  
  # Check that the _SUCCESS file was created in the output directory.
  # file_processing.sh sets OUTPUT_FILE_DIR based on BASE_OUTPUT_DIR and BASE_FILE_NAME.
  # For our test, BASE_FILE_NAME would be "test_valid_20250101".
  output_dir="$BASE_OUTPUT_DIR/test_valid_20250101"
  test -d "$output_dir" || fail "Output directory $output_dir was not created."
  
  # Verify that the _SUCCESS file exists indicating successful Spark job simulation.
  test -f "$output_dir/_SUCCESS" || fail "_SUCCESS file not found in $output_dir."
}

####################################
# Integration Test: file_processing.sh for contract_uid_mapping_table (skip upload)
####################################
@test "file_processing.sh processes contract_uid_mapping_table without upload" {
  input_file="$tmp_dir/contract_uid_mapping_table.csv"
  echo "header" > "$input_file"
  echo "data" >> "$input_file"
  
  run "$BASE_APP_DIR/file_processing.sh" "$input_file"
  [ "$status" -eq 0 ] || fail "contract_uid_mapping_table processing did not succeed."
  
  # For contract_uid_mapping_table, the upload is skipped so
  # we expect the job to succeed and then cleanup the input file.
  test ! -f "$input_file" || fail "contract_uid_mapping_table input file was not cleaned up."
}
