"""Integration tests for the pipeline."""

import pytest
import json
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime
from timescaledb_client import TimescaleDBClient
from azure_client import AzureEventHubsClient
from checkpoint_store import AzureBlobCheckpointStore
from azure_iothub_orchestrator import AzureIoTHubOrchestrator


class TestPipelineInit:
    """Test pipeline initialization."""

    def test_init_with_all_components(self):
        """Test pipeline initialization with all components."""
        db_client = Mock(spec=TimescaleDBClient)
        azure_client = Mock(spec=AzureEventHubsClient)
        checkpoint_store = Mock(spec=AzureBlobCheckpointStore)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
            checkpoint_store=checkpoint_store,
        )

        assert pipeline.db_client is db_client
        assert pipeline.azure_client is azure_client
        assert pipeline.checkpoint_store is checkpoint_store
        assert pipeline.current_batch == []
        assert pipeline.insert_counter == 0
        assert pipeline.message_count == 0

    def test_init_without_checkpoint_store(self):
        """Test pipeline initialization without checkpoint store."""
        db_client = Mock(spec=TimescaleDBClient)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        assert pipeline.db_client is db_client
        assert pipeline.azure_client is azure_client
        assert pipeline.checkpoint_store is None


class TestEventProcessing:
    """Test event processing."""

    def test_process_event_single_sensor(self):
        """Test processing event with single sensor."""
        db_client = Mock(spec=TimescaleDBClient)
        db_client.get_or_create_sensor = Mock(return_value=1)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        # Create mock event
        event = Mock()
        event.body_as_str = Mock(
            return_value='{"unix_time": 1700000000, "temps": {"28-44815f1e64ff": 22.5}}'
        )
        event.system_properties = {b'iothub-connection-device-id': 'device-001'}

        result = pipeline._process_event(event)

        assert result is True
        assert len(pipeline.current_batch) == 1
        db_client.get_or_create_sensor.assert_called_once_with("28-44815f1e64ff", "device-001")

    def test_process_event_multiple_sensors(self):
        """Test processing event with multiple sensors."""
        db_client = Mock(spec=TimescaleDBClient)
        db_client.get_or_create_sensor = Mock(side_effect=[1, 2])
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        event = Mock()
        event.body_as_str = Mock(
            return_value='{"unix_time": 1700000000, "temps": {"sensor-1": 22.5, "sensor-2": 23.1}}'
        )
        event.system_properties = {b'iothub-connection-device-id': 'device-002'}

        result = pipeline._process_event(event)

        assert result is True
        assert len(pipeline.current_batch) == 2
        assert db_client.get_or_create_sensor.call_count == 2

    def test_process_event_no_temps(self):
        """Test processing event without temperature data."""
        db_client = Mock(spec=TimescaleDBClient)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        event = Mock()
        event.body_as_str = Mock(return_value='{"unix_time": 1700000000, "temps": {}}')
        event.system_properties = {b'iothub-connection-device-id': 'device-003'}

        result = pipeline._process_event(event)

        assert result is False
        assert len(pipeline.current_batch) == 0

    def test_process_event_invalid_json(self):
        """Test processing event with invalid JSON."""
        db_client = Mock(spec=TimescaleDBClient)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        event = Mock()
        event.body_as_str = Mock(return_value="invalid json")
        event.system_properties = {b'iothub-connection-device-id': 'device-004'}

        result = pipeline._process_event(event)

        assert result is False
        assert len(pipeline.current_batch) == 0

    def test_process_event_sensor_creation_failure(self):
        """Test processing when sensor creation fails."""
        db_client = Mock(spec=TimescaleDBClient)
        db_client.get_or_create_sensor = Mock(return_value=None)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        event = Mock()
        event.body_as_str = Mock(
            return_value='{"unix_time": 1700000000, "temps": {"28-bad": 22.5}}'
        )
        event.system_properties = {b'iothub-connection-device-id': 'device-005'}

        result = pipeline._process_event(event)

        assert result is False
        assert len(pipeline.current_batch) == 0

    def test_process_event_default_timestamp(self):
        """Test processing event without unix_time (uses current time)."""
        db_client = Mock(spec=TimescaleDBClient)
        db_client.get_or_create_sensor = Mock(return_value=1)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        event = Mock()
        event.body_as_str = Mock(
            return_value='{"temps": {"28-44815f1e64ff": 22.5}}'
        )
        event.system_properties = {b'iothub-connection-device-id': 'device-006'}

        result = pipeline._process_event(event)

        assert result is True
        assert len(pipeline.current_batch) == 1


class TestBatchOperations:
    """Test batch operations."""

    def test_flush_batch_success(self):
        """Test successful batch flush."""
        db_client = Mock(spec=TimescaleDBClient)
        db_client.insert_sensor_data_batch = Mock(return_value=True)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )
        pipeline.current_batch = [("2023-11-15 10:00:00", 1, 22.5)] * 5

        result = pipeline._flush_batch()

        assert result is True
        assert len(pipeline.current_batch) == 0
        assert pipeline.insert_counter == 1
        db_client.insert_sensor_data_batch.assert_called_once()

    def test_flush_batch_empty(self):
        """Test flushing empty batch."""
        db_client = Mock(spec=TimescaleDBClient)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        result = pipeline._flush_batch()

        assert result is True
        db_client.insert_sensor_data_batch.assert_not_called()

    def test_flush_batch_failure(self):
        """Test batch flush failure."""
        db_client = Mock(spec=TimescaleDBClient)
        db_client.insert_sensor_data_batch = Mock(return_value=False)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )
        pipeline.current_batch = [("2023-11-15 10:00:00", 1, 22.5)]

        result = pipeline._flush_batch()

        assert result is False
        assert len(pipeline.current_batch) == 1

    def test_auto_flush_on_batch_full(self):
        """Test automatic flush when batch reaches MAX_BATCH_SIZE."""
        db_client = Mock(spec=TimescaleDBClient)
        db_client.get_or_create_sensor = Mock(return_value=1)
        db_client.insert_sensor_data_batch = Mock(return_value=True)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        # Add exactly MAX_BATCH_SIZE items
        event = Mock()
        event.body_as_str = Mock(
            return_value='{"unix_time": 1700000000, "temps": {"28-44815f1e64ff": 22.5}}'
        )
        event.system_properties = {b'iothub-connection-device-id': 'device-007'}

        for i in range(TimescaleDBClient.MAX_BATCH_SIZE):
            pipeline._process_event(event)

        assert len(pipeline.current_batch) == 0
        assert pipeline.insert_counter == 1


class TestCheckpointOperations:
    """Test checkpoint operations."""

    def test_update_checkpoint_success(self):
        """Test successful checkpoint update (deprecated method)."""
        db_client = Mock(spec=TimescaleDBClient)
        azure_client = Mock(spec=AzureEventHubsClient)
        checkpoint_store = Mock(spec=AzureBlobCheckpointStore)
        checkpoint_store.update_checkpoint = Mock(return_value=True)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
            checkpoint_store=checkpoint_store,
        )

        event = Mock()
        event.offset = 100
        event.sequence_number = 1

        # _update_checkpoint() is deprecated, now returns False
        result = pipeline._update_checkpoint("0", event)

        assert result is False
        # The deprecated method no longer calls checkpoint_store.update_checkpoint
        checkpoint_store.update_checkpoint.assert_not_called()

    def test_update_checkpoint_no_store(self):
        """Test checkpoint update when no store configured."""
        db_client = Mock(spec=TimescaleDBClient)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
            checkpoint_store=None,
        )

        event = Mock()
        event.offset = 100
        event.sequence_number = 1

        result = pipeline._update_checkpoint("0", event)

        assert result is False

    def test_update_checkpoint_failure(self):
        """Test checkpoint update failure."""
        db_client = Mock(spec=TimescaleDBClient)
        azure_client = Mock(spec=AzureEventHubsClient)
        checkpoint_store = Mock(spec=AzureBlobCheckpointStore)
        checkpoint_store.update_checkpoint = Mock(return_value=False)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
            checkpoint_store=checkpoint_store,
        )

        event = Mock()
        event.offset = 100
        event.sequence_number = 1

        result = pipeline._update_checkpoint("0", event)

        assert result is False


class TestOnEventBatch:
    """Test event batch callback."""

    def test_on_event_batch_success(self):
        """Test successful event batch processing."""
        db_client = Mock(spec=TimescaleDBClient)
        db_client.get_or_create_sensor = Mock(return_value=1)
        db_client.insert_sensor_data_batch = Mock(return_value=True)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        # Create mock events
        events = []
        for i in range(5):
            event = Mock()
            event.body_as_str = Mock(
                return_value=f'{{"unix_time": {1700000000 + i}, "temps": {{"sensor-{i}": {20.0 + i}}}}}'
            )
            event.system_properties = {b'iothub-connection-device-id': f'device-{i}'}
            event.offset = 100 + i
            event.sequence_number = i
            events.append(event)

        partition_context = Mock()
        partition_context.partition_id = "0"
        partition_context.update_checkpoint = Mock()

        pipeline.on_event_batch(partition_context, events)

        assert pipeline.message_count == 5
        partition_context.update_checkpoint.assert_called_once()

    def test_on_event_batch_empty(self):
        """Test processing empty event batch."""
        db_client = Mock(spec=TimescaleDBClient)
        azure_client = Mock(spec=AzureEventHubsClient)

        pipeline = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
        )

        partition_context = Mock()

        pipeline.on_event_batch(partition_context, [])

        assert pipeline.message_count == 0
        partition_context.update_checkpoint.assert_not_called()
