"""TimescaleDB client for sensor data operations.

This module encapsulates all database operations for managing sensors
and inserting sensor data into TimescaleDB hypertables.
"""

import json
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

logger = logging.getLogger(__name__)


class TimescaleDBClient:
    """Client for TimescaleDB sensor data operations."""

    DB_TABLE = "sensor_data"
    DB_COLUMNS = ["time", "sensor_id", "temperature"]
    SENSORS_TABLE = "sensors"
    MAX_BATCH_SIZE = 100

    def __init__(self, host: str, user: str, password: str, database: str, port: str = "5432"):
        """Initialize TimescaleDB client with connection parameters.
        
        Args:
            host: Database host
            user: Database user
            password: Database password
            database: Database name
            port: Database port (default: 5432)
            
        Raises:
            psycopg2.Error: If connection fails
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.conn = None
        self.sensor_cache = {}

    def connect(self) -> bool:
        """Establish database connection.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            self.conn = psycopg2.connect(
                database=self.database,
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
            )
            logger.info(
                f"Connected to TimescaleDB at {self.host}:{self.port}/{self.database}"
            )
            self._load_sensor_cache()
            return True
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}", exc_info=True)
            self.conn = None
            return False

    def disconnect(self) -> bool:
        """Close database connection.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if self.conn:
                self.conn.close()
            logger.info("Disconnected from TimescaleDB")
            return True
        except Exception as e:
            logger.error(f"Error disconnecting from database: {e}", exc_info=True)
            return False

    def is_connected(self) -> bool:
        """Check if database connection is active.
        
        Returns:
            True if connected, False otherwise
        """
        return self.conn is not None

    def _load_sensor_cache(self) -> bool:
        """Load existing sensors from database into memory cache.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected():
            logger.error("Database not connected, cannot load sensor cache")
            return False

        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT id, sensor_SN FROM {self.SENSORS_TABLE}")
            for sensor_id, sensor_SN in cursor.fetchall():
                self.sensor_cache[sensor_SN] = sensor_id
            cursor.close()
            logger.info(f"Loaded {len(self.sensor_cache)} sensors into cache")
            return True
        except Exception as e:
            logger.error(f"Error loading sensor cache: {e}", exc_info=True)
            return False

    def get_or_create_sensor(self, sensor_SN: str, device_ID: str = None) -> int:
        """Get sensor ID by serial number, or create if it doesn't exist.
        
        Args:
            sensor_SN: The sensor's serial number
            device_ID: The IoT Hub device ID (from iothub-connection-device-id)
            
        Returns:
            The sensor ID, or None if operation failed
        """
        # Check cache first
        if sensor_SN in self.sensor_cache:
            return self.sensor_cache[sensor_SN]

        if not self.is_connected():
            logger.error("Database not connected, cannot create sensor")
            return None

        try:
            cursor = self.conn.cursor()
            cursor.execute(
                f"""
                INSERT INTO {self.SENSORS_TABLE} (sensor_SN, device_ID, type, location)
                VALUES (%s, %s, 'temperature', 'unknown')
                ON CONFLICT (sensor_SN) DO UPDATE SET device_ID = EXCLUDED.device_ID
                RETURNING id;
                """,
                (sensor_SN, device_ID),
            )
            sensor_id = cursor.fetchone()[0]
            self.conn.commit()
            cursor.close()

            # Cache it
            self.sensor_cache[sensor_SN] = sensor_id
            logger.info(f"Created/retrieved sensor {sensor_SN} (device_ID: {device_ID}) with ID {sensor_id}")
            return sensor_id
        except Exception as e:
            logger.error(
                f"Error getting/creating sensor {sensor_SN}: {e}", exc_info=True
            )
            self.conn.rollback()
            return None

    def insert_sensor_data_batch(self, data: list) -> bool:
        """Insert a batch of sensor data into the database.
        
        Args:
            data: List of tuples (time, sensor_id, temperature)
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected() or not data:
            return False

        try:
            cursor = self.conn.cursor()
            sql = f"""
            INSERT INTO {self.DB_TABLE} ({','.join(self.DB_COLUMNS)})
            VALUES %s;"""
            execute_values(cursor, sql, data)
            self.conn.commit()
            cursor.close()
            logger.debug(f"Inserted {len(data)} records into {self.DB_TABLE}")
            return True
        except Exception as e:
            logger.error(f"Error inserting batch: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            return False

    def get_sensor_count(self) -> int:
        """Get total number of sensors.
        
        Returns:
            Number of sensors, or -1 if query failed
        """
        if not self.is_connected():
            return -1

        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.SENSORS_TABLE}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Error counting sensors: {e}", exc_info=True)
            return -1

    def get_sensor_data_count(self) -> int:
        """Get total number of sensor data points.
        
        Returns:
            Number of data points, or -1 if query failed
        """
        if not self.is_connected():
            return -1

        try:
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {self.DB_TABLE}")
            count = cursor.fetchone()[0]
            cursor.close()
            return count
        except Exception as e:
            logger.error(f"Error counting sensor data: {e}", exc_info=True)
            return -1

    def get_sensors(self) -> list:
        """Get all sensors from database.
        
        Returns:
            List of sensor tuples (id, sensor_SN, device_ID, type, location)
        """
        if not self.is_connected():
            return []

        try:
            cursor = self.conn.cursor()
            cursor.execute(
                f"SELECT id, sensor_SN, device_ID, type, location FROM {self.SENSORS_TABLE} ORDER BY id"
            )
            sensors = cursor.fetchall()
            cursor.close()
            return sensors
        except Exception as e:
            logger.error(f"Error retrieving sensors: {e}", exc_info=True)
            return []

    def insert_device_status(self, device_ID: str, timestamp: datetime, status_data: dict) -> bool:
        """Insert device status data into the device_status table.
        
        Args:
            device_ID: The IoT Hub device ID
            timestamp: TIMESTAMPTZ for the event
            status_data: Dictionary containing status properties
                Expected keys: eventCategory, gitVersion, version, stableVersion,
                messageCounter, numTempSensors, numFlowSensors, isCameraEnabled
            
        Returns:
            True if successful, False otherwise
        """
        if not self.is_connected() or not status_data:
            return False

        try:
            # Extract status properties with defaults
            event_category = status_data.get('eventCategory', None)
            message_counter = status_data.get('messageCounter', None)
            git_version = status_data.get('gitVersion', None)
            git_current_version = status_data.get('version', None)
            git_stable_version = status_data.get('stableVersion', None)
            num_temp_sensors = status_data.get('numTempSensors', None)
            num_flow_sensors = status_data.get('numFlowSensors', None)
            is_camera_enabled = status_data.get('isCameraEnabled', None)

            cursor = self.conn.cursor()
            sql = """
            INSERT INTO device_status (
                time, device_ID, event_category, message_counter, git_version,
                git_current_version, git_stable_version, num_temp_sensors, num_flow_sensors,
                is_camera_enabled, status_data
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            cursor.execute(
                sql,
                (
                    timestamp,
                    device_ID,
                    event_category,
                    message_counter,
                    git_version,
                    git_current_version,
                    git_stable_version,
                    num_temp_sensors,
                    num_flow_sensors,
                    is_camera_enabled,
                    json.dumps(status_data),  # Store full payload as JSONB
                ),
            )
            self.conn.commit()
            cursor.close()
            logger.debug(f"Inserted device status for {device_ID} at {timestamp}")
            return True
        except Exception as e:
            logger.error(f"Error inserting device status: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()
            return False
