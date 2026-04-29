# Testing Guidelines

This document outlines the procedures for testing both Bash and Python components of the Data Encryption Pipeline.

## Testing Overview

Our test suite is divided into two parts:

- **Bash Tests**: Run using [bats-core](https://github.com/bats-core/bats-core) for testing shell scripts.
- **Python Tests**: Run using `pytest` framework.

## Bash Testing

### Requirements

- [Bats-Core](https://github.com/bats-core/bats-core) must be installed.

### Running Bash Tests

All Bash test files are located in the `tests/bash/` directory. To run the tests, execute:

```bash
bats tests/bash/
```
#### Example Bash Test
An example Bash test (`./tests/bash/test_file_monitoring.bats`) might look like:
```bats
#!/usr/bin/env bats

setup() {
  # Set up a temporary directory for testing.
  export MONITOR_DIR="/some/test/directory"
}

@test "file_monitoring.sh detects a new file" {
  # Simulate placing a new file in MONITOR_DIR and assert that it is detected, added to the queue, and processed using `file_processing.sh`.
  run ./bin/file_monitoring.sh
  [ "\$status" -eq 0 ]
}
```

## Python Testing

### Requirements
```bash
pip install pytest
```
### Running Python Tests
```bash
pytest tests/python/
```

#### Example Test:

An example python pytest might look like `./tests/python/test_cc_encode.py`
```python
import pytest
from python.cc_encode import cc_encode

def test_cc_encode_success():
    # Assuming cc_encode returns True on successful encryption
    result = cc_encode("sample_input_file.csv")
    assert result is True

def test_cc_encode_failure():
    # Test that cc_encode raises a ValueError if provided with bad input
    with pytest.raises(ValueError):
        cc_encode("bad_input_file.csv")
```