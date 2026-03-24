-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;

-- Create Table
CREATE TABLE metrics (
  created timestamptz default now() not null,
  type_id integer not null,
  value double precision not null
);

-- Convert it to hypertable
SELECT create_hypertable('metrics', 'created');

-- Add compression capability
ALTER TABLE metrics SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'type_id',
  timescaledb.compress_orderby = 'created DESC'
);

-- Add compression policy 
SELECT add_compression_policy('metrics', INTERVAL '7 days');
