"""Orchestrator for Azure IoT Hub to TimescaleDB ingestion.

This module orchestrates the connections and processing flow between
Azure Event Hubs and TimescaleDB, managing message flow and checkpoints.
"""

import json
import logging
import os
import time
from datetime import datetime
from timescaledb_client import TimescaleDBClient
from azure_client import AzureEventHubsClient
from checkpoint_store import AzureBlobCheckpointStore

logger = logging.getLogger(__name__)


class AzureIoTHubOrchestrator:
    """Orchestrates IoT data ingestion from Azure to TimescaleDB."""

    def __init__(
        self,
        db_client: TimescaleDBClient,
        azure_client: AzureEventHubsClient,
        checkpoint_store: AzureBlobCheckpointStore = None,
    ):
        """Initialize the orchestrator with clients.
        
        Args:
            db_client: TimescaleDBClient instance
            azure_client: AzureEventHubsClient instance
            checkpoint_store: Optional checkpoint store (AzureBlobCheckpointStore)
        """
        self.db_client = db_client
        self.azure_client = azure_client
        self.checkpoint_store = checkpoint_store
        self.current_batch = []
        self.insert_counter = 0
        self.message_count = 0
        
        # Checkpoint frequency control
        checkpoint_freq_minutes = os.getenv("CHECKPOINT_UPDATE_FREQUENCY_MINUTES", "30")
        try:
            self.checkpoint_frequency_minutes = float(checkpoint_freq_minutes)
            self.checkpoint_frequency_seconds = self.checkpoint_frequency_minutes * 60
        except ValueError:
            logger.warning(
                f"Invalid CHECKPOINT_UPDATE_FREQUENCY_MINUTES value '{checkpoint_freq_minutes}', "
                f"defaulting to 30 minutes"
            )
            self.checkpoint_frequency_minutes = 30.0
            self.checkpoint_frequency_seconds = 30 * 60
        
        self.pending_checkpoints = {}
        self.last_checkpoint_write_time = time.time()
        
        # Event position strategy (ignore_past mode)
        self.ignore_past_mode = os.getenv("EVENTHUB_IGNORE_PAST", "false").lower()
        if self.ignore_past_mode not in ["false", "true", "full"]:
            logger.warning(
                f"Invalid EVENTHUB_IGNORE_PAST value '{self.ignore_past_mode}', "
                f"defaulting to 'false'"
            )
            self.ignore_past_mode = "false"
        
        logger.info("Initialized AzureIoTHubOrchestrator")
        logger.info(f"Checkpoint frequency: {self.checkpoint_frequency_minutes} minutes")
        logger.info(f"Event position strategy (EVENTHUB_IGNORE_PAST): {self.ignore_past_mode}")

    def on_event_batch(self, partition_context, events):
        """Callback for incoming event batches from Event Hubs.
        
        Args:
            partition_context: Partition context containing partition ID
            events: List of events (messages)
        """
        if not events:
            return

        try:
            partition_id = partition_context.partition_id
            for event in events:
                self._process_event(event)
                self.message_count += 1

            # Log progress every 100 messages
            if self.message_count % 100 == 0:
                logger.info(f"Processed {self.message_count} messages total")

            # Track latest checkpoint for this partition
            if self.checkpoint_store and events:
                last_event = events[-1]
                self.pending_checkpoints[partition_id] = {
                    'offset': last_event.offset,
                    'sequence_number': last_event.sequence_number,
                    'timestamp': datetime.utcnow().isoformat(),
                }
            
            # Check if time to flush pending checkpoints
            current_time = time.time()
            if current_time - self.last_checkpoint_write_time >= self.checkpoint_frequency_seconds:
                self._flush_pending_checkpoints()

            # Checkpoint after processing batch (Event Hub SDK's own checkpoint)
            partition_context.update_checkpoint(events[-1])

        except Exception as e:
            logger.error(f"Error processing event batch: {e}", exc_info=True)

    def _process_event(self, event) -> bool:
        """Process a single event message, routing to appropriate handler.
        
        Args:
            event: Azure Event Hubs event object
            
        Returns:
            True if successful, False if error
        """
        try:
            # Extract device ID from system properties
            device_ID = None
            if event.system_properties:
                device_id_bytes = event.system_properties.get(b'iothub-connection-device-id')
                # Handle both bytes and string values
                if device_id_bytes:
                    device_ID = device_id_bytes.decode('utf-8') if isinstance(device_id_bytes, bytes) else device_id_bytes
            
            if not device_ID:
                logger.warning(
                    f"device_ID not found in event system properties. "
                    f"Available keys: {list(event.system_properties.keys()) if event.system_properties else 'None'}"
                )
                device_ID = "unknown"
            
            # Get the message body
            message_body = event.body_as_str()
            payload = json.loads(message_body)
            logger.debug(f"Received message from device {device_ID}: {payload}")

            # Extract timestamp
            unix_time = int(payload.get("unix_time", 0))
            if unix_time == 0:
                logger.warning("unix_time not found in message, using current time")
                timestamp = datetime.utcnow()
            else:
                timestamp = datetime.utcfromtimestamp(unix_time)

            # Determine message type and route to appropriate handler
            if "temps" in payload:
                # Temperature sensor data
                return self._process_temperature_data(device_ID, timestamp, payload)
            elif "status" in payload:
                # Device status data
                return self._process_status_data(device_ID, timestamp, payload)
            else:
                logger.warning(f"Unknown message format from device {device_ID}: {payload}")
                return False

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in message: {e}", exc_info=True)
            return False
        except Exception as e:
            logger.error(f"Error processing event: {e}", exc_info=True)
            return False

    def _process_temperature_data(self, device_ID: str, timestamp: datetime, payload: dict) -> bool:
        """Process temperature sensor data message.
        
        Args:
            device_ID: The IoT Hub device ID
            timestamp: The event timestamp
            payload: The message payload containing 'temps' key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract temperature data
            temps = payload.get("temps", {})
            if not temps:
                logger.warning("No temperature data found in message")
                return False

            # Track batch size before processing
            batch_size_before = len(self.current_batch)

            # Process each sensor's temperature
            for sensor_SN, temperature in temps.items():
                # Get or create sensor with device_ID
                sensor_id = self.db_client.get_or_create_sensor(sensor_SN, device_ID)
                if sensor_id is None:
                    logger.warning(f"Skipping sensor {sensor_SN} due to database error")
                    continue

                # Add to batch
                data = (timestamp, sensor_id, float(temperature))
                self.current_batch.append(data)
                logger.debug(
                    f"Added sensor {sensor_SN} (ID: {sensor_id}): "
                    f"{temperature}°C at {timestamp}"
                )

            # If no sensors were added, return False
            if len(self.current_batch) == batch_size_before:
                logger.warning(f"No temperature sensors successfully processed for device {device_ID}")
                return False

            # Insert if batch is full
            max_batch = TimescaleDBClient.MAX_BATCH_SIZE
            if len(self.current_batch) >= max_batch:
                self._flush_batch()

            return True

        except Exception as e:
            logger.error(f"Error processing temperature data: {e}", exc_info=True)
            return False

    def _process_status_data(self, device_ID: str, timestamp: datetime, payload: dict) -> bool:
        """Process device status message.
        
        Args:
            device_ID: The IoT Hub device ID
            timestamp: The event timestamp
            payload: The message payload containing 'status' key
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract status information
            status = payload.get("status", {})
            if not status:
                logger.warning("No status data found in message")
                return False

            # Validate and sanitize data before database insertion
            if not self._validate_status_data(device_ID, status):
                logger.warning(f"Status data validation failed for device {device_ID}")
                return False

            # Insert status data into database
            success = self.db_client.insert_device_status(device_ID, timestamp, status)
            if success:
                logger.debug(
                    f"Stored device status for {device_ID}: "
                    f"category={status.get('eventCategory')}, "
                    f"counter={status.get('messageCounter')}"
                )
            else:
                logger.error(f"Failed to insert device status for {device_ID}")

            return success

        except Exception as e:
            logger.error(f"Error processing status data: {e}", exc_info=True)
            return False

    def _flush_batch(self) -> bool:
        """Flush any data in the current batch to database.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.current_batch:
            return True

        success = self.db_client.insert_sensor_data_batch(self.current_batch)
        if success:
            self.insert_counter += 1
            logger.info(
                f"Batch insert #{self.insert_counter} ({len(self.current_batch)} records)"
            )
            self.current_batch = []
        else:
            logger.error("Failed to flush batch to database")

        return success

    def _update_checkpoint(self, partition_id: str, event) -> bool:
        """DEPRECATED: Use _flush_pending_checkpoints() instead.
        
        This method is kept for backward compatibility.
        """
        logger.warning("_update_checkpoint() is deprecated, use frequency-based flushing")
        return False

    def _flush_pending_checkpoints(self) -> bool:
        """Flush all pending checkpoints to Azure Blob Storage.
        
        Writes all partition offsets accumulated since last flush.
        Updates last_checkpoint_write_time on successful flush.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.checkpoint_store or not self.pending_checkpoints:
            return True

        try:
            flush_count = 0
            for partition_id, checkpoint_data in self.pending_checkpoints.items():
                success = self.checkpoint_store.update_checkpoint(
                    partition_id=partition_id,
                    offset=checkpoint_data['offset'],
                    sequence_number=checkpoint_data['sequence_number'],
                    timestamp=checkpoint_data['timestamp'],
                )
                if success:
                    flush_count += 1
                else:
                    logger.error(f"Failed to flush checkpoint for partition {partition_id}")

            if flush_count > 0:
                self.last_checkpoint_write_time = time.time()
                logger.info(f"Flushed checkpoints for {flush_count} partitions")
                self.pending_checkpoints.clear()
                return True

            return False

        except Exception as e:
            logger.error(f"Error flushing pending checkpoints: {e}", exc_info=True)
            return False

    def flush_remaining(self) -> bool:
        """Flush any remaining data in the batch.
        
        Returns:
            True if successful, False otherwise
        """
        return self._flush_batch()

    def _validate_status_data(self, device_ID: str, status: dict) -> bool:
        """Validate status data format before database insertion.
        
        Args:
            device_ID: The IoT Hub device ID
            status: Status data dictionary
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Validate device_ID length (VARCHAR(64))
            if not isinstance(device_ID, str) or len(device_ID) > 64:
                logger.error(
                    f"Invalid device_ID: length {len(device_ID)} exceeds 64 chars"
                )
                return False

            # Validate string fields with length constraints
            string_field_lengths = {
                'eventCategory': 50,
                'gitVersion': 64,
                'version': 64,
                'stableVersion': 64,
            }
            
            for field, max_length in string_field_lengths.items():
                value = status.get(field)
                if value is not None:
                    if not isinstance(value, str):
                        logger.error(
                            f"Field '{field}' must be string, got {type(value).__name__}"
                        )
                        return False
                    if len(value) > max_length:
                        logger.error(
                            f"Field '{field}' length {len(value)} exceeds {max_length} chars"
                        )
                        return False

            # Validate integer fields
            integer_fields = ['messageCounter', 'numTempSensors', 'numFlowSensors']
            for field in integer_fields:
                value = status.get(field)
                if value is not None:
                    if not isinstance(value, int):
                        logger.error(
                            f"Field '{field}' must be integer, got {type(value).__name__}"
                        )
                        return False

            # Validate boolean field
            is_camera = status.get('isCameraEnabled')
            if is_camera is not None:
                if not isinstance(is_camera, bool):
                    logger.error(
                        f"Field 'isCameraEnabled' must be boolean, got {type(is_camera).__name__}"
                    )
                    return False

            return True

        except Exception as e:
            logger.error(f"Error validating status data: {e}", exc_info=True)
            return False

    def start(self) -> bool:
        """Start the ingestion orchestrator.
        
        Connects to Azure Event Hubs and starts consuming messages.
        The starting_position is determined by EVENTHUB_IGNORE_PAST setting:
        - "false": Resume from checkpoint (or beginning if no checkpoint)
        - "true": Only new events (latest)
        - "full": All events from beginning of retention period
        
        Returns:
            True if orchestrator completed, False if error
        """
        logger.info("=" * 80)
        logger.info("Azure IoT Hub to TimescaleDB Orchestrator Started")
        logger.info("=" * 80)

        # Verify connections
        if not self.db_client.is_connected():
            logger.error("Database client not connected")
            return False

        if not self.azure_client.is_connected():
            logger.error("Azure Event Hubs client not connected")
            return False

        # Initialize checkpoint store if provided
        if self.checkpoint_store:
            if not self.checkpoint_store.create_container_if_not_exists():
                logger.warning("Failed to create checkpoint container, continuing without checkpoints")
                self.checkpoint_store = None

        # Determine starting position based on EVENTHUB_IGNORE_PAST
        starting_position = self._determine_starting_position()

        # Start receiving messages
        success = self.azure_client.start_receiving(
            on_event_batch_callback=self.on_event_batch,
            starting_position=starting_position,
            batch_size=10,
            max_wait_time=5,
        )

        return success

    def _determine_starting_position(self) -> int:
        """Determine Event Hub starting position based on ignore_past_mode.
        
        Returns:
            starting_position value for Azure Event Hub consumer
        """
        if self.ignore_past_mode == "true":
            logger.info("EVENTHUB_IGNORE_PAST=true: Starting from latest events only")
            return -1  # Latest messages

        elif self.ignore_past_mode == "full":
            logger.info("EVENTHUB_IGNORE_PAST=full: Starting from beginning of retention period")
            return 0  # Beginning of retention (Azure SDK semantics)

        else:  # "false"
            if self.checkpoint_store:
                # Try to load checkpoints from blob store
                logger.info("EVENTHUB_IGNORE_PAST=false: Attempting to resume from checkpoints")
                # Note: Actual per-partition offset application happens in Event Hub SDK
                # For now, let Event Hub handle partition-specific starting positions
                # The orchestrator will read first available if no checkpoint exists
                return -2  # Beginning of available messages (default for "false")
            else:
                logger.info(
                    "EVENTHUB_IGNORE_PAST=false: No checkpoint store configured, "
                    "starting from beginning of retention"
                )
                return 0  # Beginning of retention

    def shutdown(self):
        """Gracefully shutdown the orchestrator."""
        logger.info("Shutting down orchestrator...")

        # Flush remaining batch items
        self.flush_remaining()
        
        # Force flush any pending checkpoints
        if self.pending_checkpoints:
            logger.info("Flushing pending checkpoints on shutdown...")
            self._flush_pending_checkpoints()

        # Disconnect from Azure
        self.azure_client.disconnect()

        # Disconnect from database
        self.db_client.disconnect()

        logger.info("=" * 80)
        logger.info("Azure IoT Hub Orchestrator Stopped")
        logger.info("=" * 80)
        logger.info(f"Total messages processed: {self.message_count}")
        logger.info(f"Total batch inserts: {self.insert_counter}")
