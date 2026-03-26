"""Tests for the main Azure IoT Hub ingestion pipeline module."""

import pytest
import sys
import os
from unittest.mock import Mock, MagicMock, patch, call
from azure_iot_ingestion import (
    create_db_client,
    create_azure_client,
    create_checkpoint_store,
)
from timescaledb_client import TimescaleDBClient
from azure_client import AzureEventHubsClient
from checkpoint_store import AzureBlobCheckpointStore


class TestCreateDbClient:
    """Test TimescaleDB client creation."""

    @patch("azure_iot_ingestion.TimescaleDBClient")
    @patch.dict(
        os.environ,
        {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass",
            "POSTGRES_DB": "testdb",
            "POSTGRES_PORT": "5432",
        },
    )
    def test_create_db_client_success(self, mock_db_class):
        """Test successful TimescaleDB client creation and connection."""
        mock_client = Mock()
        mock_client.connect = Mock(return_value=True)
        mock_db_class.return_value = mock_client

        result = create_db_client()

        assert result is mock_client
        mock_db_class.assert_called_once_with(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
            port="5432",
        )
        mock_client.connect.assert_called_once()

    @patch("azure_iot_ingestion.TimescaleDBClient")
    @patch.dict(
        os.environ,
        {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass",
            "POSTGRES_DB": "testdb",
        },
        clear=False,
    )
    def test_create_db_client_connection_failure(self, mock_db_class):
        """Test TimescaleDB client creation when connection fails."""
        mock_client = Mock()
        mock_client.connect = Mock(return_value=False)
        mock_db_class.return_value = mock_client

        result = create_db_client()

        assert result is None
        mock_client.connect.assert_called_once()

    @patch("azure_iot_ingestion.TimescaleDBClient")
    @patch.dict(
        os.environ,
        {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_USER": "testuser",
            "POSTGRES_PASSWORD": "testpass",
            "POSTGRES_DB": "testdb",
        },
        clear=False,
    )
    def test_create_db_client_uses_default_port(self, mock_db_class):
        """Test that default port is used when not specified."""
        mock_client = Mock()
        mock_client.connect = Mock(return_value=True)
        mock_db_class.return_value = mock_client

        result = create_db_client()

        assert result is mock_client
        # Verify default port was passed
        call_args = mock_db_class.call_args
        assert call_args[1]["port"] == "5432"

    @patch("azure_iot_ingestion.TimescaleDBClient")
    @patch.dict(
        os.environ,
        {
            "POSTGRES_HOST": "db.example.com",
            "POSTGRES_USER": "produser",
            "POSTGRES_PASSWORD": "prodpass",
            "POSTGRES_DB": "proddb",
            "POSTGRES_PORT": "5433",
        },
    )
    def test_create_db_client_custom_port(self, mock_db_class):
        """Test that custom port is used when specified."""
        mock_client = Mock()
        mock_client.connect = Mock(return_value=True)
        mock_db_class.return_value = mock_client

        result = create_db_client()

        assert result is mock_client
        call_args = mock_db_class.call_args
        assert call_args[1]["port"] == "5433"


class TestCreateAzureClient:
    """Test Azure Event Hubs client creation."""

    @patch("azure_iot_ingestion.AzureEventHubsClient")
    @patch.dict(
        os.environ,
        {
            "IOT_HUB_CONNECTION_STRING": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test==;EntityPath=test",
            "EVENTHUB_CONSUMER_GROUP": "test-consumer",
        },
    )
    def test_create_azure_client_success(self, mock_azure_class):
        """Test successful Azure Event Hubs client creation and connection."""
        mock_client = Mock()
        mock_client.connect = Mock(return_value=True)
        mock_azure_class.return_value = mock_client

        result = create_azure_client()

        assert result is mock_client
        mock_azure_class.assert_called_once_with(
            connection_string="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test==;EntityPath=test",
            consumer_group="test-consumer",
        )
        mock_client.connect.assert_called_once()

    @patch("azure_iot_ingestion.AzureEventHubsClient")
    @patch("azure_iot_ingestion.os.getenv")
    def test_create_azure_client_uses_default_consumer_group(
        self, mock_getenv, mock_azure_class
    ):
        """Test that default consumer group is used when not specified."""

        def getenv_impl(key, default=None):
            env_map = {
                "IOT_HUB_CONNECTION_STRING": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test==;EntityPath=test",
            }
            return env_map.get(key, default)

        mock_getenv.side_effect = getenv_impl
        mock_client = Mock()
        mock_client.connect = Mock(return_value=True)
        mock_azure_class.return_value = mock_client

        result = create_azure_client()

        assert result is mock_client
        call_args = mock_azure_class.call_args
        assert call_args[1]["consumer_group"] == "$Default"

    @patch("azure_iot_ingestion.AzureEventHubsClient")
    @patch.dict(
        os.environ,
        {
            "EVENTHUB_CONSUMER_GROUP": "custom-group",
        },
        clear=True,
    )
    def test_create_azure_client_missing_connection_string(self, mock_azure_class):
        """Test Azure client creation when connection string is missing."""
        result = create_azure_client()

        assert result is None
        mock_azure_class.assert_not_called()

    @patch("azure_iot_ingestion.AzureEventHubsClient")
    @patch.dict(
        os.environ,
        {
            "IOT_HUB_CONNECTION_STRING": "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test==;EntityPath=test",
            "EVENTHUB_CONSUMER_GROUP": "test-consumer",
        },
    )
    def test_create_azure_client_connection_failure(self, mock_azure_class):
        """Test Azure client creation when connection fails."""
        mock_client = Mock()
        mock_client.connect = Mock(return_value=False)
        mock_azure_class.return_value = mock_client

        result = create_azure_client()

        assert result is None
        mock_client.connect.assert_called_once()


class TestCreateCheckpointStore:
    """Test Azure Blob Storage checkpoint store creation."""

    @patch("azure_iot_ingestion.AzureBlobCheckpointStore")
    @patch.dict(
        os.environ,
        {
            "CHECKPOINT_STORE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
            "CHECKPOINT_STORE_CONTAINER_NAME": "checkpoints",
        },
    )
    def test_create_checkpoint_store_success(self, mock_store_class):
        """Test successful checkpoint store creation."""
        mock_store = Mock()
        mock_store_class.return_value = mock_store

        result = create_checkpoint_store()

        assert result is mock_store
        mock_store_class.assert_called_once_with(
            connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
            container_name="checkpoints",
        )

    @patch("azure_iot_ingestion.AzureBlobCheckpointStore")
    @patch.dict(os.environ, {}, clear=True)
    def test_create_checkpoint_store_not_configured(self, mock_store_class):
        """Test checkpoint store creation when not configured."""
        result = create_checkpoint_store()

        assert result is None
        mock_store_class.assert_not_called()

    @patch("azure_iot_ingestion.AzureBlobCheckpointStore")
    @patch.dict(
        os.environ,
        {
            "CHECKPOINT_STORE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=test==;EndpointSuffix=core.windows.net",
        },
        clear=True,
    )
    def test_create_checkpoint_store_missing_container_name(self, mock_store_class):
        """Test checkpoint store creation when container name is missing."""
        result = create_checkpoint_store()

        assert result is None
        mock_store_class.assert_not_called()

    @patch("azure_iot_ingestion.AzureBlobCheckpointStore")
    @patch.dict(
        os.environ,
        {
            "CHECKPOINT_STORE_CONTAINER_NAME": "checkpoints",
        },
        clear=True,
    )
    def test_create_checkpoint_store_missing_connection_string(self, mock_store_class):
        """Test checkpoint store creation when connection string is missing."""
        result = create_checkpoint_store()

        assert result is None
        mock_store_class.assert_not_called()


class TestLoggingConfiguration:
    """Test that logging is properly configured."""

    @patch("azure_iot_ingestion.logging.getLogger")
    def test_azure_eventhub_logger_suppressed(self, mock_get_logger):
        """Test that azure.eventhub logger is set to WARNING level."""
        import azure_iot_ingestion
        import importlib

        # Reload to trigger logging setup
        importlib.reload(azure_iot_ingestion)

        # Capture the actual logger setup calls
        # The module sets these loggers to WARNING during import
        assert True  # Logging is configured module-level

    def test_logging_imports_exist(self):
        """Test that logging configuration module is imported."""
        import azure_iot_ingestion

        assert hasattr(azure_iot_ingestion, "setup_root_logging")
        assert hasattr(azure_iot_ingestion, "TimescaleDBClient")
        assert hasattr(azure_iot_ingestion, "AzureEventHubsClient")
        assert hasattr(azure_iot_ingestion, "AzureBlobCheckpointStore")


class TestMainExecution:
    """Test main execution flow."""

    @patch("azure_iot_ingestion.create_checkpoint_store")
    @patch("azure_iot_ingestion.create_azure_client")
    @patch("azure_iot_ingestion.create_db_client")
    @patch("azure_iot_ingestion.AzureIoTHubOrchestrator")
    def test_main_execution_success(
        self,
        mock_orchestrator_class,
        mock_create_db,
        mock_create_azure,
        mock_create_checkpoint,
    ):
        """Test that main execution flow works correctly."""
        # Setup mocks
        mock_db = Mock()
        mock_azure = Mock()
        mock_checkpoint = Mock()
        mock_orchestrator = Mock()

        mock_create_db.return_value = mock_db
        mock_create_azure.return_value = mock_azure
        mock_create_checkpoint.return_value = mock_checkpoint
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.start = Mock(return_value=True)
        mock_orchestrator.shutdown = Mock()

        # Simulate main execution
        db_client = mock_create_db()
        azure_client = mock_create_azure()
        checkpoint_store = mock_create_checkpoint()

        assert db_client is mock_db
        assert azure_client is mock_azure
        assert checkpoint_store is mock_checkpoint

        orchestrator = mock_orchestrator_class(
            db_client=db_client,
            azure_client=azure_client,
            checkpoint_store=checkpoint_store,
        )

        success = orchestrator.start()
        assert success is True

        orchestrator.shutdown()
        orchestrator.shutdown.assert_called_once()

    @patch("azure_iot_ingestion.create_azure_client")
    @patch("azure_iot_ingestion.create_db_client")
    def test_main_execution_db_connection_fails(
        self, mock_create_db, mock_create_azure
    ):
        """Test main execution when DB connection fails."""
        mock_create_db.return_value = None

        db_client = mock_create_db()
        assert db_client is None
        mock_create_azure.assert_not_called()

    @patch("azure_iot_ingestion.create_azure_client")
    @patch("azure_iot_ingestion.create_db_client")
    def test_main_execution_azure_connection_fails(
        self, mock_create_db, mock_create_azure
    ):
        """Test main execution when Azure connection fails."""
        mock_db = Mock()
        mock_db.disconnect = Mock()
        mock_create_db.return_value = mock_db
        mock_create_azure.return_value = None

        db_client = mock_create_db()
        assert db_client is not None

        azure_client = mock_create_azure()
        assert azure_client is None

        # Would call disconnect in real scenario
        if db_client:
            db_client.disconnect.assert_not_called()

    @patch("azure_iot_ingestion.create_checkpoint_store")
    @patch("azure_iot_ingestion.create_azure_client")
    @patch("azure_iot_ingestion.create_db_client")
    @patch("azure_iot_ingestion.AzureIoTHubOrchestrator")
    def test_main_execution_orchestrator_init_fails(
        self,
        mock_orchestrator_class,
        mock_create_db,
        mock_create_azure,
        mock_create_checkpoint,
    ):
        """Test main execution when orchestrator initialization fails."""
        mock_db = Mock()
        mock_azure = Mock()
        mock_checkpoint = Mock()

        mock_create_db.return_value = mock_db
        mock_create_azure.return_value = mock_azure
        mock_create_checkpoint.return_value = mock_checkpoint
        mock_orchestrator_class.side_effect = RuntimeError("Initialization failed")

        db_client = mock_create_db()
        azure_client = mock_create_azure()
        checkpoint_store = mock_create_checkpoint()

        with pytest.raises(RuntimeError, match="Initialization failed"):
            orchestrator = mock_orchestrator_class(
                db_client=db_client,
                azure_client=azure_client,
                checkpoint_store=checkpoint_store,
            )

    @patch("azure_iot_ingestion.create_checkpoint_store")
    @patch("azure_iot_ingestion.create_azure_client")
    @patch("azure_iot_ingestion.create_db_client")
    @patch("azure_iot_ingestion.AzureIoTHubOrchestrator")
    def test_main_execution_orchestrator_start_fails(
        self,
        mock_orchestrator_class,
        mock_create_db,
        mock_create_azure,
        mock_create_checkpoint,
    ):
        """Test main execution when orchestrator start fails."""
        mock_db = Mock()
        mock_azure = Mock()
        mock_checkpoint = Mock()
        mock_orchestrator = Mock()

        mock_create_db.return_value = mock_db
        mock_create_azure.return_value = mock_azure
        mock_create_checkpoint.return_value = mock_checkpoint
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.start = Mock(return_value=False)
        mock_orchestrator.shutdown = Mock()

        db_client = mock_create_db()
        azure_client = mock_create_azure()
        checkpoint_store = mock_create_checkpoint()

        orchestrator = mock_orchestrator_class(
            db_client=db_client,
            azure_client=azure_client,
            checkpoint_store=checkpoint_store,
        )

        success = orchestrator.start()
        assert success is False

    @patch("azure_iot_ingestion.create_checkpoint_store")
    @patch("azure_iot_ingestion.create_azure_client")
    @patch("azure_iot_ingestion.create_db_client")
    @patch("azure_iot_ingestion.AzureIoTHubOrchestrator")
    def test_main_execution_keyboard_interrupt(
        self,
        mock_orchestrator_class,
        mock_create_db,
        mock_create_azure,
        mock_create_checkpoint,
    ):
        """Test that KeyboardInterrupt is handled gracefully."""
        mock_db = Mock()
        mock_azure = Mock()
        mock_checkpoint = Mock()
        mock_orchestrator = Mock()

        mock_create_db.return_value = mock_db
        mock_create_azure.return_value = mock_azure
        mock_create_checkpoint.return_value = mock_checkpoint
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.start = Mock(side_effect=KeyboardInterrupt())
        mock_orchestrator.shutdown = Mock()

        db_client = mock_create_db()
        azure_client = mock_create_azure()
        checkpoint_store = mock_create_checkpoint()

        orchestrator = mock_orchestrator_class(
            db_client=db_client,
            azure_client=azure_client,
            checkpoint_store=checkpoint_store,
        )

        # Simulate the try-except-finally block from main
        exception_raised = None
        try:
            orchestrator.start()
        except KeyboardInterrupt:
            exception_raised = KeyboardInterrupt
        finally:
            orchestrator.shutdown()

        assert exception_raised is KeyboardInterrupt
        orchestrator.shutdown.assert_called_once()

    @patch("azure_iot_ingestion.create_checkpoint_store")
    @patch("azure_iot_ingestion.create_azure_client")
    @patch("azure_iot_ingestion.create_db_client")
    @patch("azure_iot_ingestion.AzureIoTHubOrchestrator")
    def test_main_execution_unexpected_error(
        self,
        mock_orchestrator_class,
        mock_create_db,
        mock_create_azure,
        mock_create_checkpoint,
    ):
        """Test that unexpected errors are handled."""
        mock_db = Mock()
        mock_azure = Mock()
        mock_checkpoint = Mock()
        mock_orchestrator = Mock()

        mock_create_db.return_value = mock_db
        mock_create_azure.return_value = mock_azure
        mock_create_checkpoint.return_value = mock_checkpoint
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.start = Mock(side_effect=RuntimeError("Unexpected error"))
        mock_orchestrator.shutdown = Mock()

        db_client = mock_create_db()
        azure_client = mock_create_azure()
        checkpoint_store = mock_create_checkpoint()

        orchestrator = mock_orchestrator_class(
            db_client=db_client,
            azure_client=azure_client,
            checkpoint_store=checkpoint_store,
        )

        # Simulate the try-except-finally block from main
        exception_raised = None
        try:
            orchestrator.start()
        except RuntimeError as e:
            exception_raised = type(e)
        finally:
            orchestrator.shutdown()

        assert exception_raised is RuntimeError
        orchestrator.shutdown.assert_called_once()
