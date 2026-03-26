# Testing Guide - Azure IoT Hub to TimescaleDB Ingestion Pipeline

Complete testing guide for the modular IoT ingestion pipeline. Get started in 5 minutes or dive into comprehensive reference materials.

## Quick Start (5 Minutes)

### Prerequisites

```bash
# Ensure you're in the project directory
cd 3.IoT_data_ingestion

# Install dependencies (includes pytest)
pip install -r requirements.txt
```

### Run All Tests

```bash
pytest
```

**Output:**
```
test_timescaledb_client.py::TestTimescaleDBClientInit::test_init PASSED
test_azure_client.py::TestAzureEventHubsClientInit::test_init PASSED
test_orchestrator_status.py::TestStatusMessageProcessing::test_process_status_message_success PASSED
...
========================= 122 passed in 1.13s ==========================
```

### With Coverage Report

```bash
pytest --cov
```

## Project Structure

The ingestion pipeline consists of independently testable components:

```
3.IoT_data_ingestion/
├── azure_iot_ingestion.py              # Main entry point
├── azure_iothub_orchestrator.py        # Pipeline orchestrator
├── timescaledb_client.py               # TimescaleDB operations
├── azure_client.py                     # Azure Event Hubs operations
├── checkpoint_store.py                 # Blob Storage checkpoint management
│
├── test/
│   ├── test_azure_iot_ingestion.py    # Entry point tests (21 tests, 100% coverage)
│   ├── test_azure_iothub_orchestrator.py   # Orchestrator tests (34+ tests, 100%)
│   ├── test_timescaledb_client.py     # Database tests (30+ tests, 100%)
│   ├── test_azure_client.py           # Azure client tests (20+ tests, 100%)
│   ├── test_checkpoint_store.py       # Checkpoint tests (15+ tests, 100%)
│   ├── test_pipeline.py               # Integration tests (17+ tests, 100%)
│   ├── conftest.py                    # Pytest fixtures & configuration
│   ├── pytest.ini                     # Pytest settings
│   └── README.md                      # This file
│
└── requirements.txt                    # Dependencies
```

## Core Modules

### 1. **azure_iot_ingestion.py**
Main entry point managing client creation and pipeline initialization:
- `create_db_client()` - TimescaleDB client factory
- `create_azure_client()` - Azure Event Hubs client factory
- `create_checkpoint_store()` - Azure Blob Storage checkpoint factory
- **Tests**: 21 tests covering all factory functions and error scenarios

### 2. **azure_iothub_orchestrator.py**
Orchestrates the connection between Azure Event Hubs and TimescaleDB:
- Event processing and routing (temperature vs. status messages)
- Batch management and flushing
- Checkpoint frequency control (CHECKPOINT_UPDATE_FREQUENCY_MINUTES)
- Event position strategy (EVENTHUB_IGNORE_PAST: false/true/full)
- Data validation before database insertion
- **Tests**: 34+ tests including checkpoint frequency, event position strategy, and status message handling

### 3. **timescaledb_client.py**
Encapsulates all database operations:
- Connection management (`connect()`, `disconnect()`, `is_connected()`)
- Sensor management (`get_or_create_sensor()`, sensor caching)
- Data insertion (`insert_sensor_data_batch()`, `insert_device_status()`)
- Query operations (`get_sensor_count()`, `get_sensor_data_count()`, `get_sensors()`)
- **Tests**: 30+ tests covering connections, sensors, batch inserts, and queries

### 4. **azure_client.py**
Encapsulates Azure Event Hubs consumer:
- Connection management
- Event Hub property queries
- Message consumption setup
- Partition management
- **Tests**: 20+ tests covering connection and message receiving

### 5. **checkpoint_store.py**
Manages partition offsets in Azure Blob Storage:
- Container creation and management
- Checkpoint CRUD operations (`get_checkpoint()`, `update_checkpoint()`, `delete_checkpoint()`)
- Checkpoint listing
- **Tests**: 15+ tests covering all blob storage operations

## Running Tests

### All Tests

```bash
# Standard run
pytest

# With verbose output
pytest -v

# With coverage
pytest --cov
```

### Run Specific Components

```bash
# Database client only
pytest test_timescaledb_client.py

# Azure client only
pytest test_azure_client.py

# Checkpoint store only
pytest test_checkpoint_store.py

# Orchestrator and integration
pytest test_azure_iothub_orchestrator.py

# Pipeline integration
pytest test_pipeline.py

# Main entry point tests
pytest test_azure_iot_ingestion.py
```

### Run Specific Test Classes or Functions

```bash
# Specific class
pytest test_timescaledb_client.py::TestTimescaleDBConnection -v

# Specific test
pytest test_azure_iothub_orchestrator.py::TestCheckpointFrequencyControl::test_checkpoint_write_frequency

# Tests matching a pattern
pytest -k "checkpoint" -v
```

### Advanced Options

```bash
# Run and stop on first failure
pytest -x

# Run with detailed output
pytest -v --tb=long

# Run with keyword filtering
pytest -k "status or checkpoint" -v

# Show print statements during tests
pytest -s

# Run quietly
pytest -q
```

## Test Coverage

### Coverage by Module

| Module | Coverage |
|--------|----------|
| test_azure_iot_ingestion.py | 100% |
| test_orchestrator_status.py | 100% |
| test_pipeline.py | 100% |
| test_timescaledb_client.py | 100% |
| test_azure_client.py | 100% |
| test_checkpoint_store.py | 100% |
| **Overall** | **88%** |

### View Coverage Report

```bash
pytest --cov
```

## Test Organization

### Test Categories

#### Checkpoint & Frequency Control (TestCheckpointFrequencyControl)
Tests CHECKPOINT_UPDATE_FREQUENCY_MINUTES environment variable:
- Checkpoint write frequency enforcement
- Pending checkpoint tracking
- Force-flush on shutdown
- Fractional minute support

#### Event Position Strategy (TestEventPositionStrategy)
Tests EVENTHUB_IGNORE_PAST environment variable with three modes:
- `false` - Resume from checkpoint or beginning
- `true` - Only consume new events (latest)
- `full` - All events from retention period

#### Status Message Processing (TestStatusMessageProcessing)
Tests device status message handling:
- Status message routing
- Optional field handling
- Full payload JSONB storage
- Combined temperature and status processing

#### Database Operations
- Connection/disconnection
- Sensor creation and caching
- Batch insertion
- Query operations
- Device status insertion

#### Azure Operations
- Event Hub connection
- Event property retrieval
- Message receiving setup
- Error handling

#### Checkpoint Store Operations
- Container creation
- CRUD operations (Create, Read, Update, Delete)
- JSON serialization
- Blob operations

#### Pipeline Integration
- Event processing (single/multiple sensors)
- Batch operations (auto-flush, manual flush)
- Checkpoint updates
- Error recovery

## Testing Best Practices

### 1. Mock External Dependencies

```python
# ✅ Good: Mock database connection
db_client = Mock(spec=TimescaleDBClient)
db_client.connect = Mock(return_value=True)

# ❌ Don't: Use real database
db_client = TimescaleDBClient("localhost", ...)
```

### 2. Use Fixtures for Common Setup

```python
# conftest.py
@pytest.fixture
def mock_db_connection():
    """Create a mock database connection."""
    conn = Mock()
    conn.cursor = Mock(return_value=Mock())
    conn.commit = Mock()
    return conn

# test_file.py
def test_something(mock_db_connection):
    # Use the fixture
    pass
```

### 3. Test Both Success and Failure Cases

```python
def test_connect_success(self):
    """Test successful connection."""
    result = client.connect()
    assert result is True

def test_connect_failure(self):
    """Test connection failure handling."""
    result = client.connect()
    assert result is False
```

### 4. Verify Dependencies Are Called Correctly

```python
mock_client.method.assert_called_once_with("expected_arg")
mock_client.method.assert_called_once()
mock_client.method.assert_not_called()
```

### 5. Use Monkeypatch for Environment Variables

```python
def test_env_var_handling(self, monkeypatch):
    """Test environment variable handling."""
    monkeypatch.setenv("EVENTHUB_IGNORE_PAST", "true")
    # Test code here
```

## Understanding Test Results

### Success
```
test_pipeline.py::TestEventProcessing::test_process_event_single_sensor PASSED
```
✅ Test passed - logic works correctly

### Failure
```
test_pipeline.py::TestEventProcessing::test_process_event_invalid FAILED
AssertionError: assert False == True
```
❌ Test failed - assertions didn't pass

### Error
```
test_pipeline.py::test_something ERROR
ImportError: cannot import name 'Something'
```
⚠️ Tests couldn't run - check dependencies or imports

## Troubleshooting

### ImportError: cannot import name '...'

Ensure all modules are in the project directory and dependencies are installed:

```bash
pip install -e .
pip install -r requirements.txt
```

### Timezone issues in timestamp tests

Always use UTC times:

```python
from datetime import datetime
timestamp = datetime.utcnow()  # Use UTC
```

### Test isolation issues

Use monkeypatch to clean up environment variables:

```python
def test_something(self, monkeypatch):
    """Test with clean environment."""
    monkeypatch.delenv("EVENTHUB_IGNORE_PAST", raising=False)
    # Test code here
```

### Fixture and Mock Errors

Ensure mocks match the actual module signatures:

```python
# Check the actual function signature first
from module import function
# Then mock with matching parameters
mock_object.function = Mock(return_value=expected_value)
```

## Common Test Commands

Quick reference for frequently used pytest commands:

```bash
# Run all tests silently
pytest -q

# Run tests matching pattern
pytest -k "sensor" -v

# Stop on first failure
pytest -x

# Show print statements
pytest -s

# Run single test file
pytest test_timescaledb_client.py

# Run with minimal output
pytest --tb=short
```

## Test Execution Checklist

### Unit Test Verification (No External Connections)

- [ ] **Azure IoT Ingestion** - Entry point and client factories
- [ ] **Orchestrator** - Event processing, checkpoint frequency, event position strategy
- [ ] **TimescaleDB Client** - Connections, sensor operations, batch inserts
- [ ] **Azure Client** - Connection, event consumption setup
- [ ] **Checkpoint Store** - Container ops, checkpoint CRUD
- [ ] **Pipeline Integration** - Event routing, batch operations, error handling

### Coverage Goals

- [ ] All modules have >85% line coverage
- [ ] All critical paths covered
- [ ] Error scenarios tested
- [ ] Edge cases identified and tested

## Future Testing Improvements

- [ ] Add performance benchmarks
- [ ] Add property-based testing with Hypothesis
- [ ] Add Docker-based integration tests
- [ ] Add load testing with multiple sensors
- [ ] Add E2E tests with real Azure services (optional CI step)

# Run last failed tests
pytest --lf

# Run failed tests first, then others
pytest --ff

# Run with different verbosity
pytest -vv  # Very verbose
pytest -qq  # Very quiet

# Show slowest 10 tests
pytest --durations=10

# Run in parallel (install pytest-xdist: pip install pytest-xdist)
pytest -n auto
```

## Testing Architecture

- **Unit Tests**: Each component tested in isolation using mocks
- **No External Dependencies**: Tests don't require Azure or TimescaleDB connections
- **Independent Verification**: Test Azure connection separately from database connection
- **Integration Tests**: Pipeline tests combining all components

## Test Coverage

| Component | Tests | Coverage |
|-----------|-------|----------|
| timescaledb_client.py | 30+ | >90% |
| azure_client.py | 20+ | >90% |
| checkpoint_store.py | 25+ | >85% |
| pipeline.py | 25+ | >85% |

Generate and view coverage in terminal:

```bash
# Run tests with coverage
pytest --cov
```

Terminal output shows coverage statistics for each module.

**Coverage targets:**
- timescaledb_client.py: >90%
- azure_client.py: >90%
- checkpoint_store.py: >85%
- pipeline.py: >85%

## Troubleshooting

### "ImportError: No module named 'pytest'"
```bash
pip install pytest
```

### "No tests found"
```bash
# Check you're in the right directory
pwd  # Should end with: 3.IoT_data_ingestion

# Ensure test files exist
ls test_*.py

# Try explicit path
pytest ./test_timescaledb_client.py
```

### "connection refused" or similar database error
This is expected! Tests mock external services, so you don't need:
- ✅ Azure IoT Hub running
- ✅ Azure Storage account
- ✅ TimescaleDB running

If you see these errors IN TESTS, that's a bug. Tests should handle them.

### Tests are slow
```bash
# Skip slow tests
pytest -m "not slow"

# Run in parallel
pip install pytest-xdist
pytest -n auto
```

## What Each Test File Tests

### test_timescaledb_client.py
```
✅ Can create TimescaleDBClient without connecting
✅ Can connect and disconnect from database
✅ Can cache sensors (no repeated queries)
✅ Can create new sensors
✅ Can insert data in batches
✅ Can query database statistics
```

### test_azure_client.py
```
✅ Can create AzureEventHubsClient without connecting
✅ Can connect to Event Hubs
✅ Can handle connection errors
✅ Can retrieve Event Hub properties
✅ Can set up message receiving
✅ Can handle keyboard interrupt gracefully
```

### test_checkpoint_store.py
```
✅ Can create checkpoint container
✅ Can store checkpoint as JSON blob
✅ Can retrieve checkpoint
✅ Can update checkpoint offset
✅ Can delete checkpoint
✅ Can list all checkpoints
```

### test_pipeline.py
```
✅ Can process single sensor message
✅ Can process multiple sensors in one message
✅ Can handle messages without temperature data
✅ Can handle invalid JSON
✅ Can auto-flush when batch is full
✅ Can update checkpoints
✅ Can recover from errors
```

## Next Steps

1. ✅ Run tests to verify setup: `pytest`
2. 🚀 Verify your setup works before running the full ingestion pipeline
3. 🧪 Modify tests to match your message format
4. 📊 Add integration tests with real Azure/TimescaleDB (optional)

## Tips

- **Mocking is your friend**: Tests use mocks, so you can test without real services
- **Test independently**: Test Azure without database, database without Azure
- **Fast feedback**: Tests run in seconds, get feedback quickly
- **Coverage matters**: Aim for >85% coverage of critical code
- **Test behavior, not implementation**: Tests shouldn't care how code works internally