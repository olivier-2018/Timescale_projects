# Tutorial 1. 


## Getting started

Start containers
```bash
docker compose up -d
```
This will create a timescale and grafana containers.    
The  Timescale DB will be initialized with the table and materialized views from the 'timescaledb_initdb' folder.  
If the Table is empty, it will be populated with data from the csv file from the 'energy_data' folder.  
If the table has already been populated, the load script will not execute.  
Finally, a grafana data source with the Timescale DB will be provisioned via the 'grafana_provisioning' folder.  

A manual setup of the DB and grafana is presented below.

## Manual setup the Timescale DB with data

### Copy the data to the container
```bash
# Copy the zipped file
docker cp ./energy_data/metrics.csv.gz timescaledb:/metrics.csv.gz
# Unzip the data within the container
docker exec -it timescaledb gunzip metrics.csv.gz
```

### Initialize the Timescale DB

First, connect to the Timescale DB container:
```bash
# Connect to DB from docker 
docker exec -it timescaledb psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}
```

Then, execute the SQL cmds:  
```sql
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
```

### Import the data to the DB

```sql
-- Import data
\COPY metrics FROM metrics.csv CSV;

-- Check data
SELECT time_bucket('1 day', created, 'Europe/Berlin') AS "time",
    round((last(value, created) - first(value, created)) * 100.) / 100. AS value
FROM metrics                                   
WHERE type_id = 5
GROUP BY 1;
```

## Setup analytics

### Monitor energy consumption on a day-to-day basis
```sql
-- Create a continuous aggregate for energy consumption:
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
```

### Monitor energy consumption on an hourly basis
```sql
-- Create a continuous aggregate for energy consumption
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
```

### Test DB  query
```sql
WITH per_day AS (
   SELECT
     time,
     value
   FROM kwh_day_by_day
   WHERE "time" at time zone 'Europe/Berlin' > date_trunc('month', time) - interval '1 year'
   ORDER BY 1
  ), daily AS (
      SELECT
         to_char(time, 'Dy') as day,
         value
      FROM per_day
  ), percentile AS (
      SELECT
          day,
          approx_percentile(0.50, percentile_agg(value)) as value
      FROM daily
      GROUP BY 1
      ORDER BY 1
  )
  SELECT
      d.day,
      d.ordinal,
      pd.value
  FROM unnest(array['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']) WITH ORDINALITY AS d(day, ordinal)
  LEFT JOIN percentile pd ON lower(pd.day) = lower(d.day);
```

## Setup grafana

### Create data source to Timescale

Got to http://localhost:3003  
In grafana, select Connections --> Data Sources --> click add.

### Create dashboard
```sql
WITH per_hour AS (
SELECT
time,
value
FROM kwh_hour_by_hour
WHERE "time" at time zone 'Europe/Berlin' > date_trunc('month', time) - interval '1 year'
ORDER BY 1
), hourly AS (
 SELECT
      extract(HOUR FROM time) * interval '1 hour' as hour,
      value
 FROM per_hour
)
SELECT
    hour,
    approx_percentile(0.50, percentile_agg(value)) as median,
    max(value) as maximum
FROM hourly
GROUP BY 1
ORDER BY 1;
```

## Acknowledgements:

[TigerData (Timescale)](https://www.tigerdata.com/docs/tutorials/latest/real-time-analytics-energy-consumption)


