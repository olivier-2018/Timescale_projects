-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;

-- Dim table for sensors
CREATE TABLE sensors(
  id SERIAL PRIMARY KEY,
  sensor_SN VARCHAR(50) UNIQUE NOT NULL,
  device_ID VARCHAR(64),
  type VARCHAR(50),
  location VARCHAR(50)
);

-- Create Table for sensor data
CREATE TABLE sensor_data (
  time TIMESTAMPTZ NOT NULL,
  sensor_id INTEGER NOT NULL,
  temperature DOUBLE PRECISION,
  cpu DOUBLE PRECISION,
  FOREIGN KEY (sensor_id) REFERENCES sensors (id)
);

-- Convert it to hypertable
SELECT create_hypertable('sensor_data', 'time');

-- Add compression capability
ALTER TABLE sensor_data SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'sensor_id',
  timescaledb.compress_orderby = 'time DESC'
);

-- Add compression policy 
SELECT add_compression_policy('sensor_data', INTERVAL '2 days');

-- Create Table for device status
CREATE TABLE device_status (
  time TIMESTAMPTZ NOT NULL,
  device_ID VARCHAR(64) NOT NULL,
  event_category VARCHAR(50),
  message_counter INTEGER,
  git_version VARCHAR(64),
  git_current_version VARCHAR(64),
  git_stable_version VARCHAR(64),
  num_temp_sensors INTEGER,
  num_flow_sensors INTEGER,
  is_camera_enabled BOOLEAN,
  status_data JSONB
);

-- Convert it to hypertable
SELECT create_hypertable('device_status', 'time');

-- Add compression capability
ALTER TABLE device_status SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device_ID',
  timescaledb.compress_orderby = 'time DESC'
);

-- Add compression policy
SELECT add_compression_policy('device_status', INTERVAL '2 days');

-- Add index for device_ID queries
CREATE INDEX device_status_device_id_idx ON device_status (device_ID, time DESC);