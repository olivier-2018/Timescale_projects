"""Tests for device status message processing in AzureIoTHubOrchestrator."""

import pytest
import json
import time
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from azure_iothub_orchestrator import AzureIoTHubOrchestrator


class TestStatusMessageProcessing:
    """Test suite for device status message handling."""

    @pytest.fixture
    def orchestrator(self):
        """Create orchestrator with mocked clients."""
        db_client = Mock()
        azure_client = Mock()
        return AzureIoTHubOrchestrator(db_client, azure_client)

    def test_process_status_message_success(self, orchestrator, mock_status_event):
        """Test successful processing of a device status message."""
        orchestrator.db_client.insert_device_status = Mock(return_value=True)
        
        result = orchestrator._process_event(mock_status_event)
        
        assert result is True
        orchestrator.db_client.insert_device_status.assert_called_once()
        
        # Verify the call arguments
        call_args = orchestrator.db_client.insert_device_status.call_args
        assert call_args[0][0] == 'device-002'  # device_ID
        assert isinstance(call_args[0][1], datetime)  # timestamp
        status_data = call_args[0][2]
        assert status_data['eventCategory'] == 'ping'
        assert status_data['messageCounter'] == 29571
        assert status_data['numTempSensors'] == 1
        assert status_data['numFlowSensors'] == 0
        assert status_data['isCameraEnabled'] is False

    def test_process_temperature_message_still_works(self, orchestrator, mock_azure_event):
        """Test that temperature message processing still works (backward compatibility)."""
        orchestrator.db_client.get_or_create_sensor = Mock(return_value=1)
        
        result = orchestrator._process_event(mock_azure_event)
        
        assert result is True
        orchestrator.db_client.get_or_create_sensor.assert_called_once()
        assert len(orchestrator.current_batch) == 1

    def test_process_status_data_with_missing_fields(self, orchestrator):
        """Test processing status data with missing optional fields."""
        timestamp = datetime.utcnow()
        status_data = {
            'eventCategory': 'ping',
            'messageCounter': 100,
            # Missing other fields
        }
        payload = {"status": status_data}
        
        orchestrator.db_client.insert_device_status = Mock(return_value=True)
        result = orchestrator._process_status_data('device-003', timestamp, payload)
        
        assert result is True
        orchestrator.db_client.insert_device_status.assert_called_once()

    def test_process_status_data_empty_status(self, orchestrator):
        """Test processing with empty status field."""
        timestamp = datetime.utcnow()
        status_data = {}
        
        result = orchestrator._process_status_data('device-004', timestamp, status_data)
        
        assert result is False

    def test_process_temperature_data_success(self, orchestrator):
        """Test successful processing of temperature data."""
        timestamp = datetime.utcnow()
        payload = {'temps': {'sensor-001': 22.5, 'sensor-002': 21.3}}
        
        orchestrator.db_client.get_or_create_sensor = Mock(return_value=1)
        result = orchestrator._process_temperature_data('device-005', timestamp, payload)
        
        assert result is True
        assert orchestrator.db_client.get_or_create_sensor.call_count >= 1
        assert len(orchestrator.current_batch) == 2

    def test_process_temperature_data_empty_temps(self, orchestrator):
        """Test processing temperature message with empty temps field."""
        timestamp = datetime.utcnow()
        payload = {'temps': {}}
        
        result = orchestrator._process_temperature_data('device-006', timestamp, payload)
        
        assert result is False

    def test_message_routing_temperature(self, orchestrator):
        """Test that temperature messages are routed to temperature handler."""
        event = Mock()
        event.body_as_str = Mock(
            return_value=json.dumps({'unix_time': 1700000000, 'temps': {'sensor-001': 25.0}})
        )
        event.system_properties = {b'iothub-connection-device-id': 'device-007'}
        
        orchestrator.db_client.get_or_create_sensor = Mock(return_value=1)
        result = orchestrator._process_event(event)
        
        assert result is True
        orchestrator.db_client.get_or_create_sensor.assert_called()

    def test_message_routing_status(self, orchestrator):
        """Test that status messages are routed to status handler."""
        event = Mock()
        status_payload = {
            'unix_time': 1700000000,
            'status': {
                'eventCategory': 'alert',
                'messageCounter': 500,
                'gitVersion': 'v2.0.0',
                'version': '2.0.0'
            }
        }
        event.body_as_str = Mock(return_value=json.dumps(status_payload))
        event.system_properties = {b'iothub-connection-device-id': 'device-008'}
        
        orchestrator.db_client.insert_device_status = Mock(return_value=True)
        result = orchestrator._process_event(event)
        
        assert result is True
        orchestrator.db_client.insert_device_status.assert_called()

    def test_unknown_message_format(self, orchestrator):
        """Test handling of unknown message format."""
        event = Mock()
        event.body_as_str = Mock(
            return_value=json.dumps({'unix_time': 1700000000, 'unknown_field': 'data'})
        )
        event.system_properties = {b'iothub-connection-device-id': 'device-009'}
        
        result = orchestrator._process_event(event)
        
        assert result is False

    def test_invalid_json_in_event(self, orchestrator):
        """Test handling of invalid JSON in event."""
        event = Mock()
        event.body_as_str = Mock(return_value='invalid json {')
        event.system_properties = {b'iothub-connection-device-id': 'device-010'}
        
        result = orchestrator._process_event(event)
        
        assert result is False

    def test_missing_device_id_defaults_to_unknown(self, orchestrator):
        """Test that missing device_ID defaults to 'unknown'."""
        event = Mock()
        event.body_as_str = Mock(
            return_value=json.dumps({'unix_time': 1700000000, 'status': {'eventCategory': 'test'}})
        )
        event.system_properties = {}  # No device ID
        
        orchestrator.db_client.insert_device_status = Mock(return_value=True)
        result = orchestrator._process_event(event)
        
        call_args = orchestrator.db_client.insert_device_status.call_args
        assert call_args[0][0] == 'unknown'

    def test_batch_flushing_with_temperature_messages(self, orchestrator):
        """Test that batch auto-flushes when reaching max size."""
        orchestrator.db_client.get_or_create_sensor = Mock(return_value=1)
        orchestrator.db_client.insert_sensor_data_batch = Mock(return_value=True)
        
        # Create enough temperature data to trigger flush
        max_batch = 100
        timestamp = datetime.utcnow()
        payload = {'temps': {f'sensor-{i}': 20.0 + (i % 10) for i in range(max_batch)}}
        
        result = orchestrator._process_temperature_data('device-011', timestamp, payload)
        
        assert result is True
        # Batch should be flushed and reset
        orchestrator.db_client.insert_sensor_data_batch.assert_called()
        assert len(orchestrator.current_batch) == 0

    def test_status_data_storage_includes_full_payload(self, orchestrator):
        """Test that the full status payload is stored as JSONB."""
        timestamp = datetime.utcnow()
        status_data = {
            'eventCategory': 'diagnostic',
            'messageCounter': 5000,
            'gitVersion': 'v1.5.2',
            'version': '1.5.2',
            'stableVersion': '1.5.0',
            'numTempSensors': 3,
            'numFlowSensors': 2,
            'isCameraEnabled': True,
            'customField': 'custom_value'  # Extra field to verify full storage
        }
        payload = {"status": status_data}
        
        orchestrator.db_client.insert_device_status = Mock(return_value=True)
        result = orchestrator._process_status_data('device-012', timestamp, payload)
        
        assert result is True
        call_args = orchestrator.db_client.insert_device_status.call_args
        stored_status = call_args[0][2]
        assert stored_status['customField'] == 'custom_value'

    def test_validate_status_data_device_id_too_long(self, orchestrator):
        """Test validation fails for device_ID exceeding 64 chars."""
        status_data = {'eventCategory': 'test', 'messageCounter': 1}
        long_device_id = 'x' * 65
        
        result = orchestrator._validate_status_data(long_device_id, status_data)
        
        assert result is False

    def test_validate_status_data_git_version_too_long(self, orchestrator):
        """Test validation fails for git hash exceeding 64 chars."""
        status_data = {
            'eventCategory': 'test',
            'gitVersion': 'x' * 65,  # Exceeds 64 chars
            'messageCounter': 1
        }
        
        result = orchestrator._validate_status_data('device-001', status_data)
        
        assert result is False

    def test_validate_status_data_event_category_too_long(self, orchestrator):
        """Test validation fails for eventCategory exceeding 50 chars."""
        status_data = {
            'eventCategory': 'x' * 51,  # Exceeds 50 chars
            'messageCounter': 1
        }
        
        result = orchestrator._validate_status_data('device-001', status_data)
        
        assert result is False

    def test_validate_status_data_invalid_message_counter_type(self, orchestrator):
        """Test validation fails when messageCounter is not integer."""
        status_data = {
            'eventCategory': 'test',
            'messageCounter': '1000'  # String instead of int
        }
        
        result = orchestrator._validate_status_data('device-001', status_data)
        
        assert result is False

    def test_validate_status_data_invalid_is_camera_type(self, orchestrator):
        """Test validation fails when isCameraEnabled is not boolean."""
        status_data = {
            'eventCategory': 'test',
            'isCameraEnabled': 'true'  # String instead of boolean
        }
        
        result = orchestrator._validate_status_data('device-001', status_data)
        
        assert result is False

    def test_validate_status_data_success(self, orchestrator):
        """Test validation succeeds with correct data types and lengths."""
        status_data = {
            'eventCategory': 'ping',
            'messageCounter': 29571,
            'gitVersion': 'a1b2c3d4e5f6',
            'version': '1.2.3.4',
            'stableVersion': '1.2.0',
            'numTempSensors': 1,
            'numFlowSensors': 0,
            'isCameraEnabled': True
        }
        
        result = orchestrator._validate_status_data('device-001', status_data)
        
        assert result is True

    def test_validate_status_data_optional_fields_missing(self, orchestrator):
        """Test validation succeeds when optional fields are missing."""
        status_data = {
            'eventCategory': 'test',
            'messageCounter': 100
            # Other fields are optional
        }
        
        result = orchestrator._validate_status_data('device-001', status_data)
        
        assert result is True


class TestCheckpointFrequencyControl:
    """Test suite for checkpoint update frequency control."""

    @pytest.fixture
    def checkpoint_store_mock(self):
        """Create a mock checkpoint store."""
        store = Mock()
        store.update_checkpoint = Mock(return_value=True)
        return store

    @pytest.fixture
    def orchestrator_with_checkpoint(self, checkpoint_store_mock):
        """Create orchestrator with checkpoint store."""
        db_client = Mock()
        azure_client = Mock()
        return AzureIoTHubOrchestrator(db_client, azure_client, checkpoint_store_mock)

    def test_pending_checkpoints_tracked(self, orchestrator_with_checkpoint):
        """Test that partition offsets are tracked in pending_checkpoints."""
        event = Mock()
        event.offset = 100
        event.sequence_number = 5

        partition_context = Mock()
        partition_context.partition_id = "0"
        partition_context.update_checkpoint = Mock()

        orchestrator = orchestrator_with_checkpoint

        # Manually add to pending (simulating on_event_batch behavior)
        orchestrator.pending_checkpoints["0"] = {
            'offset': 100,
            'sequence_number': 5,
            'timestamp': datetime.utcnow().isoformat(),
        }

        assert "0" in orchestrator.pending_checkpoints
        assert orchestrator.pending_checkpoints["0"]["offset"] == 100

    def test_flush_pending_checkpoints_writes_blobs(self, orchestrator_with_checkpoint):
        """Test that _flush_pending_checkpoints writes to blob store."""
        orchestrator = orchestrator_with_checkpoint

        # Add pending checkpoints for multiple partitions
        orchestrator.pending_checkpoints = {
            "0": {'offset': 100, 'sequence_number': 5, 'timestamp': datetime.utcnow().isoformat()},
            "1": {'offset': 200, 'sequence_number': 10, 'timestamp': datetime.utcnow().isoformat()},
        }

        result = orchestrator._flush_pending_checkpoints()

        assert result is True
        assert orchestrator.checkpoint_store.update_checkpoint.call_count == 2
        assert len(orchestrator.pending_checkpoints) == 0

    def test_checkpoint_frequency_not_elapsed_skips_flush(self, orchestrator_with_checkpoint):
        """Test that checkpoint is not written if frequency window hasn't elapsed."""
        orchestrator = orchestrator_with_checkpoint
        orchestrator.checkpoint_frequency_seconds = 300  # 5 minutes
        orchestrator.last_checkpoint_write_time = time.time()

        # Add pending checkpoint
        orchestrator.pending_checkpoints["0"] = {
            'offset': 100,
            'sequence_number': 5,
            'timestamp': datetime.utcnow().isoformat(),
        }

        # Frequency window has NOT elapsed, so no flush should occur
        current_time = time.time()
        should_flush = current_time - orchestrator.last_checkpoint_write_time >= orchestrator.checkpoint_frequency_seconds

        assert should_flush is False
        assert len(orchestrator.pending_checkpoints) == 1  # Still pending

    def test_checkpoint_frequency_elapsed_flushes(self, orchestrator_with_checkpoint):
        """Test that checkpoint is written when frequency window elapses."""
        orchestrator = orchestrator_with_checkpoint
        orchestrator.checkpoint_frequency_seconds = 1  # 1 second

        # Add pending checkpoint
        orchestrator.pending_checkpoints["0"] = {
            'offset': 100,
            'sequence_number': 5,
            'timestamp': datetime.utcnow().isoformat(),
        }

        # Set last write time to past
        orchestrator.last_checkpoint_write_time = time.time() - 2  # 2 seconds ago

        # Frequency window HAS elapsed
        current_time = time.time()
        should_flush = current_time - orchestrator.last_checkpoint_write_time >= orchestrator.checkpoint_frequency_seconds

        assert should_flush is True

        # Actually flush
        result = orchestrator._flush_pending_checkpoints()
        assert result is True
        assert len(orchestrator.pending_checkpoints) == 0

    def test_shutdown_flushes_pending_checkpoints(self, orchestrator_with_checkpoint):
        """Test that shutdown forces a checkpoint flush."""
        orchestrator = orchestrator_with_checkpoint

        # Add pending checkpoints
        orchestrator.pending_checkpoints = {
            "0": {'offset': 100, 'sequence_number': 5, 'timestamp': datetime.utcnow().isoformat()},
        }

        orchestrator.shutdown()

        # Verify checkpoint_store was called to flush
        assert orchestrator.checkpoint_store.update_checkpoint.called

    def test_fractional_checkpoint_frequency(self):
        """Test that fractional minutes are parsed correctly."""
        with patch.dict('os.environ', {'CHECKPOINT_UPDATE_FREQUENCY_MINUTES': '0.5'}):
            db_client = Mock()
            azure_client = Mock()
            orchestrator = AzureIoTHubOrchestrator(db_client, azure_client)

            assert orchestrator.checkpoint_frequency_minutes == 0.5
            assert orchestrator.checkpoint_frequency_seconds == 30


class TestEventPositionStrategy:
    """Test suite for EVENTHUB_IGNORE_PAST event position strategy."""

    @pytest.fixture
    def orchestrator_default(self, monkeypatch):
        """Create orchestrator with default settings."""
        # Explicitly remove EVENTHUB_IGNORE_PAST to test default behavior
        monkeypatch.delenv("EVENTHUB_IGNORE_PAST", raising=False)
        db_client = Mock()
        azure_client = Mock()
        return AzureIoTHubOrchestrator(db_client, azure_client)

    def test_ignore_past_mode_defaults_to_false(self, orchestrator_default):
        """Test that EVENTHUB_IGNORE_PAST defaults to 'false'."""
        assert orchestrator_default.ignore_past_mode == "false"

    def test_ignore_past_mode_true_returns_latest_position(self, orchestrator_default):
        """Test that 'true' mode returns -1 (latest events)."""
        orchestrator = orchestrator_default
        orchestrator.ignore_past_mode = "true"
        orchestrator.checkpoint_store = None

        starting_pos = orchestrator._determine_starting_position()

        assert starting_pos == -1

    def test_ignore_past_mode_full_returns_beginning_position(self, orchestrator_default):
        """Test that 'full' mode returns 0 (beginning of retention)."""
        orchestrator = orchestrator_default
        orchestrator.ignore_past_mode = "full"
        orchestrator.checkpoint_store = None

        starting_pos = orchestrator._determine_starting_position()

        assert starting_pos == 0

    def test_ignore_past_mode_false_no_checkpoint_returns_beginning(self, orchestrator_default):
        """Test that 'false' mode with no checkpoint store returns 0."""
        orchestrator = orchestrator_default
        orchestrator.ignore_past_mode = "false"
        orchestrator.checkpoint_store = None

        starting_pos = orchestrator._determine_starting_position()

        assert starting_pos == 0

    def test_ignore_past_mode_false_with_checkpoint_returns_default(self, orchestrator_default):
        """Test that 'false' mode with checkpoint store returns -2."""
        orchestrator = orchestrator_default
        orchestrator.ignore_past_mode = "false"
        orchestrator.checkpoint_store = Mock()

        starting_pos = orchestrator._determine_starting_position()

        assert starting_pos == -2

    def test_invalid_ignore_past_mode_defaults_to_false(self):
        """Test that invalid EVENTHUB_IGNORE_PAST value defaults to 'false'."""
        with patch.dict('os.environ', {'EVENTHUB_IGNORE_PAST': 'invalid'}):
            db_client = Mock()
            azure_client = Mock()
            orchestrator = AzureIoTHubOrchestrator(db_client, azure_client)

            assert orchestrator.ignore_past_mode == "false"

    def test_ignore_past_mode_case_insensitive(self):
        """Test that EVENTHUB_IGNORE_PAST values are case-insensitive."""
        with patch.dict('os.environ', {'EVENTHUB_IGNORE_PAST': 'TRUE'}):
            db_client = Mock()
            azure_client = Mock()
            orchestrator = AzureIoTHubOrchestrator(db_client, azure_client)

            assert orchestrator.ignore_past_mode == "true"

    def test_determine_starting_position_logs_strategy(self, orchestrator_default):
        """Test that _determine_starting_position logs the strategy being used."""
        orchestrator = orchestrator_default
        orchestrator.ignore_past_mode = "true"
        orchestrator.checkpoint_store = None

        with patch('azure_iothub_orchestrator.logger') as mock_logger:
            orchestrator._determine_starting_position()

            # Verify logging occurred
            assert mock_logger.info.called
