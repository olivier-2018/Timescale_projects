"""Unit tests for Azure Blob Storage checkpoint store."""

import pytest
import json
from unittest.mock import Mock, MagicMock, patch
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from checkpoint_store import AzureBlobCheckpointStore


class TestCheckpointStoreInit:
    """Test checkpoint store initialization."""

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_init(self, mock_container_client):
        """Test checkpoint store initialization."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client

        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )

        assert store.connection_string == "TestConnectionString"
        assert store.container_name == "test-checkpoints"
        assert store.container_client is not None


class TestContainerOperations:
    """Test container creation and management."""

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_create_container_if_not_exists_new(self, mock_container_client):
        """Test creating a new container."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_container = Mock()
        mock_container.create_container = Mock()
        store.container_client = mock_container

        result = store.create_container_if_not_exists()

        assert result is True
        mock_container.create_container.assert_called_once()

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_create_container_if_not_exists_already_exists(self, mock_container_client):
        """Test creating container that already exists."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_container = Mock()
        mock_container.create_container = Mock(side_effect=ResourceExistsError("Already exists"))
        store.container_client = mock_container

        result = store.create_container_if_not_exists()

        assert result is True

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_create_container_if_not_exists_error(self, mock_container_client):
        """Test container creation with error."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_container = Mock()
        mock_container.create_container = Mock(side_effect=Exception("Creation failed"))
        store.container_client = mock_container

        result = store.create_container_if_not_exists()

        assert result is False


class TestCheckpointRetrieval:
    """Test checkpoint retrieval operations."""

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_get_checkpoint_success(self, mock_container_client):
        """Test successful checkpoint retrieval."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        checkpoint_data = {
            "offset": 100,
            "sequence_number": 1,
            "timestamp": "2023-11-15T10:00:00",
        }
        mock_blob_client = Mock()
        mock_blob_data = Mock()
        mock_blob_data.readall = Mock(return_value=json.dumps(checkpoint_data).encode())
        mock_blob_client.download_blob = Mock(return_value=mock_blob_data)

        mock_container = Mock()
        mock_container.get_blob_client = Mock(return_value=mock_blob_client)
        store.container_client = mock_container

        result = store.get_checkpoint("0")

        assert result == checkpoint_data
        mock_container.get_blob_client.assert_called_with("checkpoint-0")

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_get_checkpoint_not_found(self, mock_container_client):
        """Test checkpoint retrieval when not found."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_blob_client = Mock()
        mock_blob_client.download_blob = Mock(side_effect=ResourceNotFoundError("Not found"))

        mock_container = Mock()
        mock_container.get_blob_client = Mock(return_value=mock_blob_client)
        store.container_client = mock_container

        result = store.get_checkpoint("0")

        assert result is None

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_get_checkpoint_invalid_json(self, mock_container_client):
        """Test checkpoint retrieval with invalid JSON."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_blob_client = Mock()
        mock_blob_data = Mock()
        mock_blob_data.readall = Mock(return_value=b"invalid json")
        mock_blob_client.download_blob = Mock(return_value=mock_blob_data)

        mock_container = Mock()
        mock_container.get_blob_client = Mock(return_value=mock_blob_client)
        store.container_client = mock_container

        result = store.get_checkpoint("0")

        assert result is None

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_get_checkpoint_error(self, mock_container_client):
        """Test checkpoint retrieval with error."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_blob_client = Mock()
        mock_blob_client.download_blob = Mock(side_effect=Exception("Read error"))

        mock_container = Mock()
        mock_container.get_blob_client = Mock(return_value=mock_blob_client)
        store.container_client = mock_container

        result = store.get_checkpoint("0")

        assert result is None


class TestCheckpointUpdate:
    """Test checkpoint update operations."""

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_update_checkpoint_success(self, mock_container_client):
        """Test successful checkpoint update."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_blob_client = Mock()
        mock_blob_client.upload_blob = Mock()

        mock_container = Mock()
        mock_container.get_blob_client = Mock(return_value=mock_blob_client)
        store.container_client = mock_container

        result = store.update_checkpoint(
            partition_id="0",
            offset=100,
            sequence_number=1,
            timestamp="2023-11-15T10:00:00",
        )

        assert result is True
        mock_blob_client.upload_blob.assert_called_once()
        # Verify the uploaded data
        call_args = mock_blob_client.upload_blob.call_args
        uploaded_data = json.loads(call_args[0][0])
        assert uploaded_data["offset"] == 100
        assert uploaded_data["sequence_number"] == 1

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_update_checkpoint_error(self, mock_container_client):
        """Test checkpoint update with error."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_blob_client = Mock()
        mock_blob_client.upload_blob = Mock(side_effect=Exception("Upload failed"))

        mock_container = Mock()
        mock_container.get_blob_client = Mock(return_value=mock_blob_client)
        store.container_client = mock_container

        result = store.update_checkpoint(
            partition_id="0",
            offset=100,
            sequence_number=1,
            timestamp="2023-11-15T10:00:00",
        )

        assert result is False


class TestCheckpointDeletion:
    """Test checkpoint deletion operations."""

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_delete_checkpoint_success(self, mock_container_client):
        """Test successful checkpoint deletion."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_blob_client = Mock()
        mock_blob_client.delete_blob = Mock()

        mock_container = Mock()
        mock_container.get_blob_client = Mock(return_value=mock_blob_client)
        store.container_client = mock_container

        result = store.delete_checkpoint("0")

        assert result is True
        mock_blob_client.delete_blob.assert_called_once()

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_delete_checkpoint_not_found(self, mock_container_client):
        """Test deletion when checkpoint not found."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_blob_client = Mock()
        mock_blob_client.delete_blob = Mock(side_effect=ResourceNotFoundError("Not found"))

        mock_container = Mock()
        mock_container.get_blob_client = Mock(return_value=mock_blob_client)
        store.container_client = mock_container

        result = store.delete_checkpoint("0")

        assert result is True  # Still returns True for not found

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_delete_checkpoint_error(self, mock_container_client):
        """Test deletion with error."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )
        mock_blob_client = Mock()
        mock_blob_client.delete_blob = Mock(side_effect=Exception("Delete failed"))

        mock_container = Mock()
        mock_container.get_blob_client = Mock(return_value=mock_blob_client)
        store.container_client = mock_container

        result = store.delete_checkpoint("0")

        assert result is False


class TestListCheckpoints:
    """Test checkpoint listing operations."""

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_list_checkpoints_success(self, mock_container_client):
        """Test successful listing of checkpoints."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )

        # Mock list_blobs
        mock_blob1 = Mock()
        mock_blob1.name = "checkpoint-0"
        mock_blob2 = Mock()
        mock_blob2.name = "checkpoint-1"
        mock_blob3 = Mock()
        mock_blob3.name = "other-data"

        mock_container = Mock()
        mock_container.list_blobs = Mock(return_value=[mock_blob1, mock_blob2, mock_blob3])

        # Mock get_checkpoint calls
        checkpoint_data_0 = {"offset": 100, "sequence_number": 1, "timestamp": "2023-11-15T10:00:00"}
        checkpoint_data_1 = {"offset": 200, "sequence_number": 2, "timestamp": "2023-11-15T10:00:01"}

        mock_blob_client = Mock()
        mock_blob_data = Mock()

        def get_blob_client_side_effect(name):
            client = Mock()
            if name == "checkpoint-0":
                data = Mock()
                data.readall = Mock(return_value=json.dumps(checkpoint_data_0).encode())
                client.download_blob = Mock(return_value=data)
            elif name == "checkpoint-1":
                data = Mock()
                data.readall = Mock(return_value=json.dumps(checkpoint_data_1).encode())
                client.download_blob = Mock(return_value=data)
            return client

        mock_container.get_blob_client = Mock(side_effect=get_blob_client_side_effect)
        store.container_client = mock_container

        result = store.list_checkpoints()

        assert len(result) == 2
        assert result[0]["partition_id"] == "0"
        assert result[1]["partition_id"] == "1"

    @patch("checkpoint_store.ContainerClient.from_connection_string")
    def test_list_checkpoints_error(self, mock_container_client):
        """Test listing checkpoints with error."""
        mock_client = Mock()
        mock_container_client.return_value = mock_client
        
        store = AzureBlobCheckpointStore(
            connection_string="TestConnectionString",
            container_name="test-checkpoints",
        )

        mock_container = Mock()
        mock_container.list_blobs = Mock(side_effect=Exception("List failed"))
        store.container_client = mock_container

        result = store.list_checkpoints()

        assert result == []
