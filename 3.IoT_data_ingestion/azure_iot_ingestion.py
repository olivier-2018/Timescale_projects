"""Main entry point for Azure IoT Hub to TimescaleDB ingestion pipeline.

This script orchestrates the connection to Azure Event Hubs and TimescaleDB,
using isolated client modules and optional checkpoint store for fault tolerance.
"""

import os
import sys
import logging
from dotenv import load_dotenv

from logging_config import setup_root_logging
from timescaledb_client import TimescaleDBClient
from azure_client import AzureEventHubsClient
from checkpoint_store import AzureBlobCheckpointStore
from azure_iothub_orchestrator import AzureIoTHubOrchestrator

load_dotenv()

# Configure logging for the entire application
setup_root_logging(
    log_file="iot_ingestion.log",
    level=logging.INFO,
)

# Suppress verbose Azure Event Hubs and AMQP protocol-level logs
logging.getLogger("azure.eventhub").setLevel(logging.WARNING)
logging.getLogger("uamqp").setLevel(logging.WARNING)
logging.getLogger("azure.eventhub._pyamqp").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


def create_db_client() -> TimescaleDBClient:
    """Create and connect to TimescaleDB.
    
    Returns:
        Connected TimescaleDBClient, or None if connection failed
    """
    logger.info("Initializing TimescaleDB client...")
    db_client = TimescaleDBClient(
        host=os.getenv("POSTGRES_HOST"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        database=os.getenv("POSTGRES_DB"),
        port=os.getenv("POSTGRES_PORT", "5432"),
    )

    if not db_client.connect():
        logger.error("Failed to connect to TimescaleDB")
        return None

    logger.info("TimescaleDB client connected successfully")
    return db_client


def create_azure_client() -> AzureEventHubsClient:
    """Create Azure Event Hubs client.
    
    Returns:
        Connected AzureEventHubsClient, or None if connection failed
    """
    logger.info("Initializing Azure Event Hubs client...")
    connection_str = os.getenv("IOT_HUB_CONNECTION_STRING")
    consumer_group = os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default")

    if not connection_str:
        logger.error("Missing IOT_HUB_CONNECTION_STRING environment variable")
        return None

    azure_client = AzureEventHubsClient(
        connection_string=connection_str,
        consumer_group=consumer_group,
    )

    if not azure_client.connect():
        logger.error("Failed to connect to Azure Event Hubs")
        return None

    logger.info(f"Azure Event Hubs client connected with consumer group '{consumer_group}'")
    return azure_client


def create_checkpoint_store() -> AzureBlobCheckpointStore:
    """Create Azure Blob Storage checkpoint store.
    
    Returns:
        AzureBlobCheckpointStore instance, or None if not configured
    """
    checkpoint_conn_str = os.getenv("CHECKPOINT_STORE_CONNECTION_STRING")
    container_name = os.getenv("CHECKPOINT_STORE_CONTAINER_NAME")

    if not checkpoint_conn_str or not container_name:
        logger.warning(
            "Checkpoint store not configured. Checkpoints will not be persisted."
        )
        return None

    logger.info("Initializing Azure Blob Storage checkpoint store...")
    checkpoint_store = AzureBlobCheckpointStore(
        connection_string=checkpoint_conn_str,
        container_name=container_name,
    )

    logger.info(f"Checkpoint store initialized with container '{container_name}'")
    return checkpoint_store


# Main execution
if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("Azure IoT Hub to TimescaleDB Ingestion Pipeline Started")
    logger.info("=" * 80)
    logger.info("Log file: iot_ingestion.log")

    # Create clients
    db_client = create_db_client()
    if not db_client:
        logger.error("Failed to create TimescaleDB client")
        sys.exit(1)

    azure_client = create_azure_client()
    if not azure_client:
        logger.error("Failed to create Azure Event Hubs client")
        db_client.disconnect()
        sys.exit(1)

    # Create optional checkpoint store
    checkpoint_store = create_checkpoint_store()

    # Create and start pipeline
    try:
        orchestrator = AzureIoTHubOrchestrator(
            db_client=db_client,
            azure_client=azure_client,
            checkpoint_store=checkpoint_store,
        )
        success = orchestrator.start()
        if not success:
            logger.error("Pipeline failed to start")
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error in pipeline: {e}", exc_info=True)
        sys.exit(1)
    finally:
        orchestrator.shutdown()
