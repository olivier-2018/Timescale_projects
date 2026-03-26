-- Create a continuous aggregate for energy consumption by HOUR
CREATE MATERIALIZED VIEW one_hour_candle
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('1 hour', time) AS bucket,
        symbol,
        FIRST(price, time) AS "open",
        MAX(price) AS high,
        MIN(price) AS low,
        LAST(price, time) AS "close",
        LAST(day_volume, time) AS day_volume
    FROM stocks_real_time
    GROUP BY bucket, symbol;

-- Add refresh policy
SELECT add_continuous_aggregate_policy('one_hour_candle',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');

-- Test
-- SELECT * FROM one_hour_candle
-- WHERE symbol = 'AAPL' AND bucket >= NOW() - INTERVAL '5 hours'
-- ORDER BY bucket;



-- Create a continuous aggregate for energy consumption by HOUR
CREATE MATERIALIZED VIEW ten_min_candle
WITH (timescaledb.continuous) AS
    SELECT
        time_bucket('10 minutes', time) AS bucket,
        symbol,
        FIRST(price, time) AS "open",
        MAX(price) AS high,
        MIN(price) AS low,
        LAST(price, time) AS "close",
        LAST(day_volume, time) AS day_volume
    FROM stocks_real_time
    GROUP BY bucket, symbol;

-- Add refresh policy
SELECT add_continuous_aggregate_policy('ten_min_candle',
    start_offset => INTERVAL '30 minutes',
    end_offset => INTERVAL '10 minutes',
    schedule_interval => INTERVAL '10 minutes');

-- Test
-- SELECT * FROM ten_min_candle
-- WHERE symbol = 'BTC/USD' AND bucket >= NOW() - INTERVAL '2 hours'
-- ORDER BY bucket;