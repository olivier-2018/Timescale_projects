"""Unit tests for TimescaleDB client."""

import pytest
import psycopg2
from unittest.mock import Mock, MagicMock, patch, call
from timescaledb_client import TimescaleDBClient


class TestTimescaleDBClientInit:
    """Test TimescaleDBClient initialization."""

    def test_init(self):
        """Test client initialization."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        assert client.host == "localhost"
        assert client.user == "testuser"
        assert client.database == "testdb"
        assert client.conn is None
        assert client.sensor_cache == {}


class TestTimescaleDBConnection:
    """Test database connection methods."""

    @patch("timescaledb_client.psycopg2.connect")
    def test_connect_success(self, mock_connect):
        """Test successful database connection."""
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor = Mock(return_value=mock_cursor)
        mock_cursor.execute = Mock()
        mock_cursor.fetchall = Mock(return_value=[])
        mock_cursor.close = Mock()
        mock_connect.return_value = mock_conn

        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        result = client.connect()

        assert result is True
        assert client.conn is not None
        mock_connect.assert_called_once()

    @patch("timescaledb_client.psycopg2.connect")
    def test_connect_failure(self, mock_connect):
        """Test failed database connection."""
        mock_connect.side_effect = psycopg2.OperationalError("Connection refused")

        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        result = client.connect()

        assert result is False
        assert client.conn is None

    def test_disconnect_success(self):
        """Test successful disconnection."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        mock_conn = Mock()
        client.conn = mock_conn

        result = client.disconnect()

        assert result is True
        mock_conn.close.assert_called_once()

    def test_is_connected_true(self):
        """Test is_connected returns True when connected."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        client.conn = Mock()
        assert client.is_connected() is True

    def test_is_connected_false(self):
        """Test is_connected returns False when not connected."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        assert client.is_connected() is False


class TestSensorCache:
    """Test sensor cache operations."""

    def test_load_sensor_cache(self):
        """Test loading sensors into cache."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor = Mock(return_value=mock_cursor)
        mock_cursor.fetchall = Mock(return_value=[(1, "28-44815f1e64ff"), (2, "28-0000000000")])
        mock_cursor.close = Mock()
        client.conn = mock_conn

        result = client._load_sensor_cache()

        assert result is True
        assert client.sensor_cache == {
            "28-44815f1e64ff": 1,
            "28-0000000000": 2,
        }

    def test_get_or_create_sensor_from_cache(self):
        """Test getting sensor from cache."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        client.sensor_cache = {"28-44815f1e64ff": 1}

        result = client.get_or_create_sensor("28-44815f1e64ff", "device-001")

        assert result == 1

    def test_get_or_create_sensor_not_connected(self):
        """Test creating sensor when not connected."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        client.conn = None

        result = client.get_or_create_sensor("28-44815f1e64ff", "device-001")

        assert result is None

    def test_get_or_create_sensor_creates_new(self):
        """Test creating new sensor."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor = Mock(return_value=mock_cursor)
        mock_cursor.fetchone = Mock(return_value=(10,))
        mock_cursor.close = Mock()
        mock_conn.commit = Mock()
        client.conn = mock_conn

        result = client.get_or_create_sensor("28-new-sensor", "device-002")

        assert result == 10
        assert client.sensor_cache["28-new-sensor"] == 10
        mock_conn.commit.assert_called_once()


class TestDataInsertion:
    """Test sensor data insertion."""

    @patch("timescaledb_client.execute_values")
    def test_insert_sensor_data_batch_success(self, mock_execute_values):
        """Test successful batch insert."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor = Mock(return_value=mock_cursor)
        mock_conn.commit = Mock()
        client.conn = mock_conn

        data = [
            ("2023-11-15 10:00:00", 1, 22.5),
            ("2023-11-15 10:00:01", 2, 23.1),
        ]

        result = client.insert_sensor_data_batch(data)

        assert result is True
        mock_execute_values.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.commit.assert_called_once()

    def test_insert_sensor_data_batch_empty(self):
        """Test batch insert with empty data."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        mock_conn = Mock()
        client.conn = mock_conn

        result = client.insert_sensor_data_batch([])

        assert result is False

    def test_insert_sensor_data_batch_not_connected(self):
        """Test batch insert when not connected."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        client.conn = None

        result = client.insert_sensor_data_batch([("2023-11-15 10:00:00", 1, 22.5)])

        assert result is False

    def test_insert_sensor_data_batch_failure(self):
        """Test batch insert failure."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor = Mock(return_value=mock_cursor)
        mock_cursor.execute = Mock(side_effect=Exception("Insert failed"))
        mock_conn.rollback = Mock()
        client.conn = mock_conn

        result = client.insert_sensor_data_batch([("2023-11-15 10:00:00", 1, 22.5)])

        assert result is False
        mock_conn.rollback.assert_called_once()


class TestQueries:
    """Test database query operations."""

    def test_get_sensor_count(self):
        """Test getting sensor count."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor = Mock(return_value=mock_cursor)
        mock_cursor.fetchone = Mock(return_value=(5,))
        mock_cursor.close = Mock()
        client.conn = mock_conn

        result = client.get_sensor_count()

        assert result == 5

    def test_get_sensor_data_count(self):
        """Test getting sensor data count."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor = Mock(return_value=mock_cursor)
        mock_cursor.fetchone = Mock(return_value=(1000,))
        mock_cursor.close = Mock()
        client.conn = mock_conn

        result = client.get_sensor_data_count()

        assert result == 1000

    def test_get_sensors(self):
        """Test getting all sensors."""
        client = TimescaleDBClient(
            host="localhost",
            user="testuser",
            password="testpass",
            database="testdb",
        )
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor = Mock(return_value=mock_cursor)
        sensors = [(1, "28-44815f1e64ff", "temperature", "room1")]
        mock_cursor.fetchall = Mock(return_value=sensors)
        mock_cursor.close = Mock()
        client.conn = mock_conn

        result = client.get_sensors()

        assert result == sensors
