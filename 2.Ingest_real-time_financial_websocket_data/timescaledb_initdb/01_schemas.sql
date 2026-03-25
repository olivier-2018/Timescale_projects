-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;

-- Create Table
CREATE TABLE stocks_real_time (
    "time" TIMESTAMPTZ,
    symbol TEXT,
    price DOUBLE PRECISION,
    day_volume NUMERIC
);

-- Convert it to hypertable
SELECT create_hypertable('stocks_real_time', 'time');

-- Add compression capability
ALTER TABLE stocks_real_time SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol',
  timescaledb.compress_orderby = 'time DESC'
);

-- Add compression policy 
SELECT add_compression_policy('stocks_real_time', INTERVAL '1 day');


-- Create Dim table
CREATE TABLE crypto_assets (
    symbol TEXT UNIQUE,
    "name" TEXT
);

