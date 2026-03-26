-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;

-- Create Table
CREATE TABLE sensor_data (
  time TIMESTAMPTZ NOT NULL,
  sensor_id INTEGER,
  temperature DOUBLE PRECISION,
  cpu DOUBLE PRECISION,
  FOREIGN KEY (sensor_id) REFERENCES sensors (id)
);

-- Convert it to hypertable
SELECT create_hypertable('sensor_data', 'time');

-- Add compression capability
ALTER TABLE metrics SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'sensor_id',
  timescaledb.compress_orderby = 'time DESC'
);

-- Add compression policy 
SELECT add_compression_policy('sensor_data', INTERVAL '2 days');

-- Add Dim table
CREATE TABLE sensors(
  id SERIAL PRIMARY KEY,
  type VARCHAR(50),
  location VARCHAR(50)
);