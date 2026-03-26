"""Unit tests for Azure Event Hubs client."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from azure.core.exceptions import AzureError
from azure_client import AzureEventHubsClient


class TestAzureEventHubsClientInit:
    """Test AzureEventHubsClient initialization."""

    def test_init_with_checkpoint_store(self):
        """Test client initialization with checkpoint store."""
        checkpoint_store = Mock()
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
            checkpoint_store=checkpoint_store,
        )
        assert client.connection_string == "TestConnectionString"
        assert client.consumer_group == "test-group"
        assert client.checkpoint_store is checkpoint_store
        assert client.client is None

    def test_init_without_checkpoint_store(self):
        """Test client initialization without checkpoint store."""
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        assert client.connection_string == "TestConnectionString"
        assert client.consumer_group == "test-group"
        assert client.checkpoint_store is None
        assert client.client is None


class TestAzureConnection:
    """Test Azure Event Hubs connection methods."""

    @patch("azure_client.EventHubConsumerClient.from_connection_string")
    def test_connect_success(self, mock_from_conn_str):
        """Test successful Event Hubs connection."""
        mock_client = Mock()
        mock_from_conn_str.return_value = mock_client

        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        result = client.connect()

        assert result is True
        assert client.client is not None
        mock_from_conn_str.assert_called_once()

    @patch("azure_client.EventHubConsumerClient.from_connection_string")
    def test_connect_azure_error(self, mock_from_conn_str):
        """Test connection failure with Azure error."""
        mock_from_conn_str.side_effect = AzureError("Authentication failed")

        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        result = client.connect()

        assert result is False
        assert client.client is None

    @patch("azure_client.EventHubConsumerClient.from_connection_string")
    def test_connect_general_error(self, mock_from_conn_str):
        """Test connection failure with general exception."""
        mock_from_conn_str.side_effect = Exception("Unexpected error")

        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        result = client.connect()

        assert result is False
        assert client.client is None

    def test_disconnect_success(self):
        """Test successful disconnection."""
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        mock_client = Mock()
        mock_client.close = Mock()
        client.client = mock_client

        result = client.disconnect()

        assert result is True
        mock_client.close.assert_called_once()

    def test_disconnect_with_error(self):
        """Test disconnection with error."""
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        mock_client = Mock()
        mock_client.close = Mock(side_effect=Exception("Close failed"))
        client.client = mock_client

        result = client.disconnect()

        assert result is False

    def test_is_connected_true(self):
        """Test is_connected returns True when connected."""
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        client.client = Mock()
        assert client.is_connected() is True

    def test_is_connected_false(self):
        """Test is_connected returns False when not connected."""
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        assert client.is_connected() is False


class TestEventHubProperties:
    """Test Event Hub property retrieval."""

    def test_get_event_hub_properties_success(self):
        """Test successful property retrieval."""
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        mock_client = Mock()
        mock_properties = {"id": "test-hub", "partition_ids": ["0", "1"]}
        mock_client.get_eventhub_properties = Mock(return_value=mock_properties)
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=False)
        client.client = mock_client

        result = client.get_event_hub_properties()

        assert result == mock_properties

    def test_get_event_hub_properties_not_connected(self):
        """Test property retrieval when not connected."""
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )

        result = client.get_event_hub_properties()

        assert result is None

    def test_get_partition_properties_success(self):
        """Test successful partition property retrieval."""
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        mock_client = Mock()
        mock_props = {
            "first_sequence_number": 0,
            "last_sequence_number": 100,
        }
        mock_client.get_partition_properties = Mock(return_value=mock_props)
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=False)
        client.client = mock_client

        result = client.get_partition_properties("0")

        assert result == mock_props
        mock_client.get_partition_properties.assert_called_once_with("0")

    def test_get_partition_properties_not_connected(self):
        """Test partition property retrieval when not connected."""
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )

        result = client.get_partition_properties("0")

        assert result is None


class TestMessageReceiving:
    """Test message receiving functionality."""

    def test_start_receiving_success(self):
        """Test successful message receiving."""
        callback = Mock()
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        mock_client = Mock()
        mock_client.receive_batch = Mock()
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=False)
        client.client = mock_client

        result = client.start_receiving(callback)

        assert result is True
        mock_client.receive_batch.assert_called_once()

    def test_start_receiving_not_connected(self):
        """Test receiving when not connected."""
        callback = Mock()
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )

        result = client.start_receiving(callback)

        assert result is False

    def test_start_receiving_with_custom_params(self):
        """Test receiving with custom parameters."""
        callback = Mock()
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        mock_client = Mock()
        mock_client.receive_batch = Mock()
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=False)
        client.client = mock_client

        result = client.start_receiving(
            callback,
            starting_position=-100,
            batch_size=20,
            max_wait_time=10,
        )

        assert result is True
        # Verify receive_batch was called with correct parameters
        call_args = mock_client.receive_batch.call_args
        assert call_args[1]["starting_position"] == -100
        assert call_args[1]["batch_size"] == 20
        assert call_args[1]["max_wait_time"] == 10

    def test_start_receiving_keyboard_interrupt(self):
        """Test receiving interrupted by user."""
        callback = Mock()
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        mock_client = Mock()
        mock_client.receive_batch = Mock(side_effect=KeyboardInterrupt())
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=False)
        client.client = mock_client

        result = client.start_receiving(callback)

        assert result is True  # Should handle gracefully

    def test_start_receiving_exception(self):
        """Test receiving with exception."""
        callback = Mock()
        client = AzureEventHubsClient(
            connection_string="TestConnectionString",
            consumer_group="test-group",
        )
        mock_client = Mock()
        mock_client.receive_batch = Mock(side_effect=Exception("Receive error"))
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=False)
        client.client = mock_client

        result = client.start_receiving(callback)

        assert result is False
