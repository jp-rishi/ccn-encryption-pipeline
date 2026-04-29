#!/usr/bin/env bats

# Setup and teardown for module tests.
setup() {
  tmp_dir=$(mktemp -d)
  export BASE_APP_DIR="$PWD"  # Adjust if needed.
  export BASE_LOG_DIR="$tmp_dir/log"
  export BASE_TEMP_DIR="$tmp_dir/temp"
  mkdir -p "$BASE_LOG_DIR" "$BASE_TEMP_DIR"
  # Create a dummy config environment, used e.g. by file_monitoring.sh and utils.
  echo "MONITOR_DIR=$tmp_dir/data" > "$tmp_dir/config.env"
  echo "WEBHOOK_URL=http://dummyurl" >> "$tmp_dir/config.env"
  echo "BASE_OUTPUT_DIR=$tmp_dir/output" >> "$tmp_dir/config.env"
  mkdir -p "$tmp_dir/data" "$tmp_dir/output"
  # Create a dummy JSON queue file.
  export QUEUE_FILE="$tmp_dir/file_queue.json"
  echo "[]" > "$QUEUE_FILE"
}

teardown() {
  rm -rf "$tmp_dir"
}

#############################
# Unit Test: locks functions
#############################
@test "acquire_lock and release_lock work correctly" {
  lock_file="$tmp_dir/test.lock"
  # Source the locks script.
  source "$BASE_APP_DIR/bin/includes/locks.sh"
  run acquire_lock "$lock_file"
  [ "$status" -eq 0 ]
  [ -f "$lock_file" ] || fail "Lock file was not created."
  run release_lock "$lock_file"
  [ "$status" -eq 0 ]
  [ ! -f "$lock_file" ] || fail "Lock file still exists."
}

####################################
# Unit Test: utils initialize_json_file
####################################
@test "initialize_json_file creates a valid JSON file" {
  test_json="$tmp_dir/test.json"
  rm -f "$test_json"
  source "$BASE_APP_DIR/bin/includes/utils.sh"
  run initialize_json_file "$test_json"
  [ "$status" -eq 0 ]
  [ -f "$test_json" ] || fail "JSON file was not created."
  run jq '.' "$test_json"
  [ "$status" -eq 0 ] || fail "File is not valid JSON."
}

####################################
# Unit Test: notify: send_webhook_notification
####################################
@test "send_webhook_notification returns success" {
  # Create a fake curl to simulate a successful HTTP call.
  fake_curl() { echo "OK"; return 0; }
  export -f fake_curl
  # Prepend the current directory so that fake_curl is found.
  PATH="$PWD:$PATH"
  
  export WEBHOOK_URL="http://dummyurl"
  source "$BASE_APP_DIR/bin/includes/notify.sh"
  
  run send_webhook_notification "Test message from Bats"
  [ "$status" -eq 0 ] || fail "send_webhook_notification did not succeed."
}
