"""Azure Blob Storage checkpoint store for Event Hubs consumer.

This module implements a checkpoint store using Azure Blob Storage to track
partition offsets and sequence numbers, allowing the consumer to resume
from the last processed message in case of service unavailability.
"""

import logging
import json
from azure.storage.blob import BlobClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

logger = logging.getLogger(__name__)


class AzureBlobCheckpointStore:
    """Manages checkpoints using Azure Blob Storage.
    
    Each partition's checkpoint is stored as a blob with JSON content:
    {
        "offset": <offset>,
        "sequence_number": <seq_num>,
        "timestamp": <iso_timestamp>
    }
    """

    def __init__(self, connection_string: str, container_name: str):
        """Initialize the checkpoint store.
        
        Args:
            connection_string: Azure Storage account connection string
            container_name: Name of the container to store checkpoints
        """
        self.connection_string = connection_string
        self.container_name = container_name
        self.container_client = ContainerClient.from_connection_string(
            conn_str=connection_string,
            container_name=container_name,
        )
        logger.info(
            f"Initialized AzureBlobCheckpointStore with container '{container_name}'"
        )

    def create_container_if_not_exists(self) -> bool:
        """Create the checkpoint container if it doesn't exist.
        
        Returns:
            True if container was created or already exists, False otherwise
        """
        try:
            self.container_client.create_container()
            logger.info(f"Created checkpoint container '{self.container_name}'")
            return True
        except ResourceExistsError:
            logger.debug(f"Checkpoint container '{self.container_name}' already exists")
            return True
        except Exception as e:
            logger.error(
                f"Error creating checkpoint container: {e}", exc_info=True
            )
            return False

    def get_checkpoint(self, partition_id: str) -> dict:
        """Retrieve checkpoint for a partition.
        
        Args:
            partition_id: The partition ID (e.g., '0', '1')
            
        Returns:
            Dictionary with keys 'offset' and 'sequence_number', or None if not found
        """
        blob_name = f"checkpoint-{partition_id}"
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            data = blob_client.download_blob().readall()
            checkpoint = json.loads(data)
            logger.debug(
                f"Retrieved checkpoint for partition {partition_id}: "
                f"offset={checkpoint.get('offset')}, "
                f"seq={checkpoint.get('sequence_number')}"
            )
            return checkpoint
        except ResourceNotFoundError:
            logger.debug(f"No checkpoint found for partition {partition_id}")
            return None
        except json.JSONDecodeError as e:
            logger.error(
                f"Error decoding checkpoint for partition {partition_id}: {e}",
                exc_info=True,
            )
            return None
        except Exception as e:
            logger.error(
                f"Error retrieving checkpoint for partition {partition_id}: {e}",
                exc_info=True,
            )
            return None

    def update_checkpoint(
        self, partition_id: str, offset: int, sequence_number: int, timestamp: str
    ) -> bool:
        """Update checkpoint for a partition.
        
        Args:
            partition_id: The partition ID
            offset: The message offset
            sequence_number: The message sequence number
            timestamp: ISO format timestamp
            
        Returns:
            True if successful, False otherwise
        """
        blob_name = f"checkpoint-{partition_id}"
        try:
            checkpoint = {
                "offset": offset,
                "sequence_number": sequence_number,
                "timestamp": timestamp,
            }
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.upload_blob(json.dumps(checkpoint), overwrite=True)
            logger.debug(
                f"Updated checkpoint for partition {partition_id}: "
                f"offset={offset}, seq={sequence_number}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Error updating checkpoint for partition {partition_id}: {e}",
                exc_info=True,
            )
            return False

    def delete_checkpoint(self, partition_id: str) -> bool:
        """Delete checkpoint for a partition (useful for testing).
        
        Args:
            partition_id: The partition ID
            
        Returns:
            True if deleted or not found, False on error
        """
        blob_name = f"checkpoint-{partition_id}"
        try:
            blob_client = self.container_client.get_blob_client(blob_name)
            blob_client.delete_blob()
            logger.info(f"Deleted checkpoint for partition {partition_id}")
            return True
        except ResourceNotFoundError:
            logger.debug(f"Checkpoint not found for partition {partition_id}")
            return True
        except Exception as e:
            logger.error(
                f"Error deleting checkpoint for partition {partition_id}: {e}",
                exc_info=True,
            )
            return False

    def list_checkpoints(self) -> list:
        """List all checkpoint blobs in the container.
        
        Returns:
            List of checkpoint dictionaries
        """
        try:
            checkpoints = []
            for blob in self.container_client.list_blobs():
                if blob.name.startswith("checkpoint-"):
                    partition_id = blob.name.replace("checkpoint-", "")
                    checkpoint = self.get_checkpoint(partition_id)
                    if checkpoint:
                        checkpoints.append({
                            "partition_id": partition_id,
                            **checkpoint
                        })
            logger.debug(f"Found {len(checkpoints)} checkpoints in container")
            return checkpoints
        except Exception as e:
            logger.error(f"Error listing checkpoints: {e}", exc_info=True)
            return []
