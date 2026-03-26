"""Pytest configuration and shared fixtures."""

import pytest
import logging
import os
from unittest.mock import Mock, MagicMock

# Suppress verbose logging during tests
logging.getLogger("azure").setLevel(logging.WARNING)
logging.getLogger("timescaledb_client").setLevel(logging.WARNING)
logging.getLogger("azure_client").setLevel(logging.WARNING)
logging.getLogger("checkpoint_store").setLevel(logging.WARNING)


@pytest.fixture
def mock_db_connection():
    """Create a mock database connection."""
    conn = Mock()
    conn.cursor = Mock(return_value=Mock())
    conn.commit = Mock()
    conn.rollback = Mock()
    conn.close = Mock()
    return conn


@pytest.fixture
def mock_azure_event():
    """Create a mock Azure Event Hubs event."""
    event = Mock()
    event.body_as_str = Mock(
        return_value='{"unix_time": 1700000000, "temps": {"28-44815f1e64ff": 22.5}}'
    )
    event.system_properties = {
        b'iothub-connection-device-id': 'device-001',
        'iothub-connection-auth-method': 'sas',
        'x-opt-sequence-number': 1,
        'x-opt-offset': '100',
        'x-opt-timestamp': 1700000000
    }
    event.offset = 100
    event.sequence_number = 1
    return event


@pytest.fixture
def mock_status_event():
    """Create a mock Azure Event Hubs event with device status message."""
    event = Mock()
    event.body_as_str = Mock(
        return_value='{"unix_time": 1700000000, "status": {"eventCategory": "ping", "gitVersion": "v1.2.3", "version": "1.2.3", "stableVersion": "1.2.0", "messageCounter": 29571, "numTempSensors": 1, "numFlowSensors": 0, "isCameraEnabled": false}}'
    )
    event.system_properties = {
        b'iothub-connection-device-id': 'device-002',
        'iothub-connection-auth-method': 'sas',
        'x-opt-sequence-number': 2,
        'x-opt-offset': '101',
        'x-opt-timestamp': 1700000000
    }
    event.offset = 101
    event.sequence_number = 2
    return event


@pytest.fixture
def mock_partition_context():
    """Create a mock partition context."""
    context = Mock()
    context.partition_id = "0"
    context.update_checkpoint = Mock()
    return context


@pytest.fixture
def db_env_vars(monkeypatch):
    """Set up database environment variables."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_PORT", "5432")


@pytest.fixture
def azure_env_vars(monkeypatch):
    """Set up Azure environment variables."""
    monkeypatch.setenv(
        "IOT_HUB_CONNECTION_STRING",
        "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test==;EntityPath=test",
    )
    monkeypatch.setenv("EVENTHUB_CONSUMER_GROUP", "test-consumer")
    monkeypatch.setenv(
        "CHECKPOINT_STORE_CONNECTION_STRING",
        "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
    )
    monkeypatch.setenv("CHECKPOINT_STORE_CONTAINER_NAME", "test-checkpoints")
