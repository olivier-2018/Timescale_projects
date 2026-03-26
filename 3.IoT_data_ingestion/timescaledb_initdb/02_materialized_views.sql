-- Enable continuous aggregates for sensor temperature monitoring
-- Drop existing views if they exist
DROP MATERIALIZED VIEW IF EXISTS sensor_temp_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS sensor_temp_30min CASCADE;
DROP MATERIALIZED VIEW IF EXISTS sensor_temp_10min CASCADE;

-- 10-minute temperature aggregates
CREATE MATERIALIZED VIEW sensor_temp_10min
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('10 minutes', time) AS bucket_time,
  sensor_id,
  COUNT(*) as reading_count,
  AVG(temperature) as avg_temperature,
  MIN(temperature) as min_temperature,
  MAX(temperature) as max_temperature,
  STDDEV(temperature) as stddev_temperature
FROM sensor_data
WHERE temperature IS NOT NULL
GROUP BY 1, 2
WITH DATA;

-- Add continuous aggregate policy for 10-minute view
SELECT add_continuous_aggregate_policy('sensor_temp_10min',
  start_offset => INTERVAL '3 hours',
  end_offset => INTERVAL '10 minutes',
  schedule_interval => INTERVAL '5 minutes');

-- 30-minute temperature aggregates
CREATE MATERIALIZED VIEW sensor_temp_30min
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('30 minutes', time) AS bucket_time,
  sensor_id,
  COUNT(*) as reading_count,
  AVG(temperature) as avg_temperature,
  MIN(temperature) as min_temperature,
  MAX(temperature) as max_temperature,
  STDDEV(temperature) as stddev_temperature
FROM sensor_data
WHERE temperature IS NOT NULL
GROUP BY 1, 2
WITH DATA;

-- Add continuous aggregate policy for 30-minute view
SELECT add_continuous_aggregate_policy('sensor_temp_30min',
  start_offset => INTERVAL '12 hours',
  end_offset => INTERVAL '30 minutes',
  schedule_interval => INTERVAL '15 minutes');

-- 1-hour temperature aggregates
CREATE MATERIALIZED VIEW sensor_temp_1h
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 hour', time) AS bucket_time,
  sensor_id,
  COUNT(*) as reading_count,
  AVG(temperature) as avg_temperature,
  MIN(temperature) as min_temperature,
  MAX(temperature) as max_temperature,
  STDDEV(temperature) as stddev_temperature
FROM sensor_data
WHERE temperature IS NOT NULL
GROUP BY 1, 2
WITH DATA;

-- Add continuous aggregate policy for 1-hour view
SELECT add_continuous_aggregate_policy('sensor_temp_1h',
  start_offset => INTERVAL '2 days',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '30 minutes');

-- Create indexes for better query performance
CREATE INDEX idx_sensor_temp_10min_sensor_time 
  ON sensor_temp_10min (sensor_id, bucket_time DESC);

CREATE INDEX idx_sensor_temp_30min_sensor_time 
  ON sensor_temp_30min (sensor_id, bucket_time DESC);

CREATE INDEX idx_sensor_temp_1h_sensor_time 
  ON sensor_temp_1h (sensor_id, bucket_time DESC);
