-- Create a continuous aggregate for energy consumption by DAY:
CREATE MATERIALIZED VIEW kwh_day_by_day(time, value)
   with (timescaledb.continuous) as
SELECT time_bucket('1 day', created, 'Europe/Berlin') AS "time",
       round((last(value, created) - first(value, created)) * 100.) / 100. AS value
FROM metrics
WHERE type_id = 5
GROUP BY 1;

-- Add refresh policy
SELECT add_continuous_aggregate_policy('kwh_day_by_day',
   start_offset => NULL,
   end_offset => INTERVAL '1 hour',
   schedule_interval => INTERVAL '1 hour');




-- Create a continuous aggregate for energy consumption by HOUR
CREATE MATERIALIZED VIEW kwh_hour_by_hour(time, value)
  with (timescaledb.continuous) as
SELECT time_bucket('01:00:00', metrics.created, 'Europe/Berlin') AS "time",
       round((last(value, created) - first(value, created)) * 100.) / 100. AS value
FROM metrics
WHERE type_id = 5
GROUP BY 1;

-- Add refresh policy
SELECT add_continuous_aggregate_policy('kwh_hour_by_hour',
 start_offset => NULL,
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');



--- Main Analysis query
-- WITH per_day AS (
--    SELECT
--      time,
--      value
--    FROM kwh_day_by_day
--    WHERE "time" at time zone 'Europe/Berlin' > date_trunc('month', time) - interval '1 year'
--    ORDER BY 1
--   ), daily AS (
--       SELECT
--          to_char(time, 'Dy') as day,
--          value
--       FROM per_day
--   ), percentile AS (
--       SELECT
--           day,
--           approx_percentile(0.50, percentile_agg(value)) as value
--       FROM daily
--       GROUP BY 1
--       ORDER BY 1
--   )
--   SELECT
--       d.day,
--       d.ordinal,
--       pd.value
--   FROM unnest(array['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']) WITH ORDINALITY AS d(day, ordinal)
--   LEFT JOIN percentile pd ON lower(pd.day) = lower(d.day);
