"""Azure Event Hubs client for consuming IoT Hub messages.

This module encapsulates Azure Event Hubs consumer functionality,
allowing independent testing of Azure connectivity and message consumption.
"""

import logging
from azure.eventhub import EventHubConsumerClient
from azure.core.exceptions import AzureError
from logging_config import setup_logging

logger = logging.getLogger(__name__)


class AzureEventHubsClient:
    """Client for consuming messages from Azure Event Hubs."""

    def __init__(
        self,
        connection_string: str,
        consumer_group: str,
        checkpoint_store=None,
    ):
        """Initialize Azure Event Hubs consumer client.
        
        Args:
            connection_string: Event Hubs connection string (from IoT Hub)
            consumer_group: Consumer group name (e.g., 'timescaledb-consumer')
            checkpoint_store: Optional checkpoint store implementation
                             (AzureBlobCheckpointStore or similar)
        """
        self.connection_string = connection_string
        self.consumer_group = consumer_group
        self.checkpoint_store = checkpoint_store
        self.client = None
        logger.info(
            f"Initialized AzureEventHubsClient with consumer group '{consumer_group}'"
        )

    def connect(self) -> bool:
        """Establish connection to Event Hubs.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.client = EventHubConsumerClient.from_connection_string(
                conn_str=self.connection_string,
                consumer_group=self.consumer_group,
            )
            logger.info(f"Connected to Event Hubs with consumer group '{self.consumer_group}'")
            return True
        except AzureError as e:
            logger.error(f"Failed to connect to Event Hubs: {e}", exc_info=True)
            self.client = None
            return False
        except Exception as e:
            logger.error(f"Unexpected error connecting to Event Hubs: {e}", exc_info=True)
            self.client = None
            return False

    def disconnect(self) -> bool:
        """Close Event Hubs connection.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.client:
                self.client.close()
            logger.info("Disconnected from Event Hubs")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from Event Hubs: {e}", exc_info=True)
            return False

    def is_connected(self) -> bool:
        """Check if Event Hubs connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        return self.client is not None

    def get_event_hub_properties(self) -> dict:
        """Get Event Hub properties (partitions, etc).
        
        Returns:
            Dictionary with properties like 'id', 'partition_ids', or None if failed
        """
        if not self.is_connected():
            logger.error("Event Hubs client not connected")
            return None

        try:
            with self.client:
                properties = self.client.get_eventhub_properties()
                logger.info(
                    f"Event Hub properties: {properties.get('id')}, "
                    f"partitions: {properties.get('partition_ids')}"
                )
                return properties
        except Exception as e:
            logger.error(f"Error getting Event Hub properties: {e}", exc_info=True)
            return None

    def get_partition_properties(self, partition_id: str) -> dict:
        """Get properties for a specific partition.
        
        Args:
            partition_id: The partition ID
            
        Returns:
            Dictionary with partition properties, or None if failed
        """
        if not self.is_connected():
            logger.error("Event Hubs client not connected")
            return None

        try:
            with self.client:
                properties = self.client.get_partition_properties(partition_id)
                logger.debug(
                    f"Partition {partition_id} properties: "
                    f"first_seq={properties.get('first_sequence_number')}, "
                    f"last_seq={properties.get('last_sequence_number')}"
                )
                return properties
        except Exception as e:
            logger.error(
                f"Error getting partition {partition_id} properties: {e}", exc_info=True
            )
            return None

    def start_receiving(
        self,
        on_event_batch_callback,
        starting_position: int = -1,
        batch_size: int = 10,
        max_wait_time: int = 5,
    ) -> bool:
        """Start receiving messages from Event Hubs.
        
        Args:
            on_event_batch_callback: Callback function for receiving batches
                                    Signature: fn(partition_context, events)
            starting_position: Starting position (-1 for latest, -100 for last 100)
            batch_size: Number of events per batch
            max_wait_time: Maximum wait time in seconds
            
        Returns:
            True if receiving loop started (blocks until interrupted)
        """
        if not self.is_connected():
            logger.error("Event Hubs client not connected")
            return False

        try:
            logger.info("Starting Event Hubs message consumption...")
            with self.client:
                self.client.receive_batch(
                    on_event_batch=on_event_batch_callback,
                    starting_position=starting_position,
                    batch_size=batch_size,
                    max_wait_time=max_wait_time,
                )
            return True
        except KeyboardInterrupt:
            logger.info("Message consumption interrupted by user")
            return True
        except Exception as e:
            logger.error(f"Error during message consumption: {e}", exc_info=True)
            return False


# Test/demo mode
if __name__ == "__main__":
    import os
    import sys
    import json
    from dotenv import load_dotenv

    load_dotenv()

    # Configure logging for testing
    test_logger = setup_logging(
        log_file="azure_client_test.log",
        level=logging.DEBUG,
        name=__name__,
    )

    test_logger.info("=" * 80)
    test_logger.info("Azure Event Hubs Client - Standalone Test Mode")
    test_logger.info("=" * 80)

    # Get connection string from environment
    connection_str = os.getenv("IOT_HUB_CONNECTION_STRING")
    consumer_group = os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default")

    if not connection_str:
        test_logger.error("Missing IOT_HUB_CONNECTION_STRING environment variable")
        sys.exit(1)

    # Create client
    test_logger.info(f"Connecting to Event Hubs with consumer group '{consumer_group}'...")
    client = AzureEventHubsClient(
        connection_string=connection_str,
        consumer_group=consumer_group,
    )

    # Try to connect
    if not client.connect():
        test_logger.error("Failed to connect to Event Hubs")
        sys.exit(1)

    test_logger.info("Connected to Event Hubs!")

    # Get Event Hub properties
    properties = client.get_event_hub_properties()
    if properties:
        test_logger.info(f"Event Hub ID: {properties.get('id')}")
        test_logger.info(f"Partitions: {properties.get('partition_ids')}")

    # Define callback to handle incoming events
    message_count = [0]  # Use list for mutability in nested function

    def on_event_batch_callback(partition_context, events):
        """Handle incoming event batch."""
        if not events:
            return

        for event in events:
            try:
                message_body = event.body_as_str()
                test_logger.info(f"[Partition {partition_context.partition_id}] Received message: {message_body}")
                
                # Log all event properties
                test_logger.debug("=" * 60)
                test_logger.debug("EVENT PROPERTIES:")
                test_logger.debug(f"  Body: {message_body}")
                
                # Application Properties (custom properties)
                if event.properties:
                    test_logger.debug("  Application Properties:")
                    for key, value in event.properties.items():
                        test_logger.debug(f"    {key}: {value}")
                else:
                    test_logger.debug("  Application Properties: None")
                
                # System Properties (device info, timestamps, etc.)
                if event.system_properties:
                    test_logger.debug("  System Properties:")
                    sys_props = event.system_properties
                    test_logger.debug(f"    device_id: {sys_props.get('iothub-connection-device-id')}")
                    test_logger.debug(f"    module_id: {sys_props.get('iothub-connection-module-id')}")
                    test_logger.debug(f"    connection_auth: {sys_props.get('iothub-connection-auth-method')}")
                    test_logger.debug(f"    enqueued_time: {sys_props.get('iothub-enqueuedtime')}")
                    test_logger.debug(f"    sequence_number: {sys_props.get('x-opt-sequence-number')}")
                    test_logger.debug(f"    offset: {sys_props.get('x-opt-offset')}")
                    test_logger.debug(f"    timestamp: {sys_props.get('x-opt-timestamp')}")
                    # Display all system properties
                    test_logger.debug("  All System Properties:")
                    for key, value in sys_props.items():
                        test_logger.debug(f"    {key}: {value}")
                else:
                    test_logger.debug("  System Properties: None")
                
                test_logger.debug("=" * 60)
                
                # Try to parse as JSON and pretty print
                try:
                    payload = json.loads(message_body)
                    # test_logger.info(f"  Parsed JSON: {json.dumps(payload, indent=2)}")
                except json.JSONDecodeError:
                    test_logger.debug(f"  (Message is not JSON)")
                
                message_count[0] += 1
            except Exception as e:
                test_logger.error(f"Error processing event: {e}", exc_info=True)

        # Update checkpoint
        partition_context.update_checkpoint(events[-1])
        test_logger.info(f"Processed batch of {len(events)} messages. Total: {message_count[0]}")

    # Start consuming messages
    try:
        test_logger.info("Starting message consumption (press Ctrl+C to stop)...")
        test_logger.info("-" * 80)
        client.start_receiving(
            on_event_batch_callback=on_event_batch_callback,
            starting_position=-1,  # Start from latest
            batch_size=10,
            max_wait_time=5,
        )
    except KeyboardInterrupt:
        test_logger.info("Message consumption interrupted by user")
    except Exception as e:
        test_logger.error(f"Error during message consumption: {e}", exc_info=True)
        sys.exit(1)
    finally:
        test_logger.info("-" * 80)
        test_logger.info(f"Test completed. Total messages received: {message_count[0]}")
        client.disconnect()
        test_logger.info("Disconnected from Event Hubs")
        test_logger.info("=" * 80)
