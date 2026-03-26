# Project 3: IoT Data Ingestion from Azure IoT Hub

This project demonstrates real-time ingestion of temperature sensor data from Azure IoT Hub into TimescaleDB.

## Overview

- **Data Source**: Azure IoT Hub (Event Hubs protocol)
- **Azure SDK**: `azure-eventhub` for reliable message consumption
- **Temperature Sensors**: Multiple sensors with serial numbers (e.g., `28-44815f1e64ff`)
- **Device Status**: Real-time device health, version, and metadata tracking
- **Storage**: TimescaleDB with hypertables and continuous aggregates for time-series optimization
- **Monitoring**: Comprehensive logging to file and console
- **Visualization**: Grafana for dashboard analytics
- **Fault Tolerance**: Azure Blob Storage checkpoints for offset tracking
- **Architecture**: Modular, independently testable components

## Architecture

The ingestion pipeline uses a modular design with separated concerns:

```
Azure Event Hubs → AzureEventHubsClient
                          ↓
                    Pipeline (Orchestrator)
                    ↙          ↓         ↘
        TimescaleDB      Checkpoint    Message
        Operations       Store         Processing
           ↓              ↓
     Database       Blob Storage
```

### Components

1. **`timescaledb_client.py`** - Database operations
   - Connection management
   - Sensor CRUD operations with caching
   - Temperature data batch insertion
   - Device status message storage
   - Query operations

2. **`azure_client.py`** - Azure Event Hubs consumer
   - Connection/disconnection
   - Event Hub property queries
   - Message consumption control
   - Partition management

3. **`checkpoint_store.py`** - Azure Blob Storage checkpoints
   - Partition offset persistence
   - Fault recovery support
   - Checkpoint CRUD operations

4. **`azure_iothub_orchestrator.py`** - Message orchestrator (formerly `pipeline.py`)
   - Dependency injection for all clients
   - Event batch processing and routing
   - Temperature data vs. device status message separation
   - Batch management and auto-flushing
   - Checkpoint coordination
   - Event position strategy (EVENTHUB_IGNORE_PAST support)

5. **`azure_iot_ingestion.py`** - Entry point
   - Environment variable loading
   - Client initialization
   - Pipeline execution

## Message Format

The script expects Event Hubs messages in the following JSON formats:

### Temperature Data Messages

```json
{
  "unix_time": "1774519751",
  "temps": {
    "28-44815f1e64ff": 20.312,
    "28-00000e75163b": 25.81,
    "28-00000967e141": 25.69
  },
  "temps_meta": {
    "target_frequency_seconds": 60
  }
}
```

**Key fields:**
- `unix_time`: Unix timestamp of the measurement
- `temps`: Dictionary of sensor serial numbers and their temperature readings (°C)
- `temps_meta`: Optional metadata about the measurement frequency

### Device Status Messages

```json
{
  "unix_time": "1774519751",
  "event_category": "device_status",
  "message_counter": 42,
  "git_version": "v1.2.3",
  "git_current_version": "v1.2.3",
  "git_stable_version": "v1.2.0",
  "num_temp_sensors": 3,
  "num_flow_sensors": 2,
  "is_camera_enabled": true,
  "status_data": {
    "uptime_seconds": 3600,
    "memory_usage": 65.5,
    "cpu_usage": 25.3
  }
}
```

**Key fields:**
- `unix_time`: Unix timestamp of the status message
- `event_category`: Message type identifier ("device_status")
- `message_counter`: Sequential message count from device
- `git_*`: Version information of device software
- `num_temp_sensors`, `num_flow_sensors`: Connected sensor counts
- `is_camera_enabled`: Camera status
- `status_data`: Flexible JSONB field for additional status information

## Getting Started

## Getting started

### Python Virtual Environment

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create a Python 3.11 environment
uv venv --python 3.11 .venv

# Activate the virtual environment
source .venv/bin/activate  # On Linux/Mac

# Install python libraries
uv pip install -r requirements.txt   
```

### 1. Get Azure IoT Hub Connection String

From Azure Portal:
1. Navigate to your IoT Hub
2. Go to **Built-in endpoints**
3. Copy the **Event Hub-compatible endpoint** connection string
4. It should look like: `Endpoint=sb://your-iot-hub.servicebus.windows.net/;SharedAccessKeyName=owner;SharedAccessKey=xxxxx;EntityPath=iothub-en-xxxxx`

### 2. Configure Environment Variables

Copy the example environment file:

```bash
cp .env.example .env
```

Edit `.env` with your values:
```
IOT_HUB_CONNECTION_STRING=Endpoint=sb://your-iot-hub.servicebus.windows.net/;SharedAccessKeyName=owner;SharedAccessKey=xxxxx;EntityPath=iothub-en-xxxxx

# Optional: For persistent checkpoint storage
CHECKPOINT_STORE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=xxxxx
CHECKPOINT_STORE_CONTAINER_NAME=my-checkpoints
EVENTHUB_CONSUMER_GROUP=my-consumer-group
```

### 3. Start Containers

```bash
docker compose up -d
```

This will:
- Create and initialize TimescaleDB with the sensor schema
- Start Grafana for visualization
- Start the Azure IoT ingestion service (connects to Azure Event Hubs)

### 4. Monitor Ingestion

View the ingestion script logs:
```bash
docker logs azure-iot-ingestion -f
```

View the detailed log file (inside container):
```bash
docker exec azure-iot-ingestion tail -f iot_ingestion.log
```

## Testing

The project includes comprehensive unit and integration tests for all components.
```bash
cd 3.IoT_data_ingestion

# Run all tests
pytest test/ -v

# Or from inside the test directory
cd test/
pytest -v

# Run specific component tests
pytest test/test_azure_client.py -v
pytest test/test_pipeline.py -v
```


## Database Schema

### Sensors Table
Stores metadata about connected temperature sensors linked to devices:

```sql
CREATE TABLE sensors(
  id SERIAL PRIMARY KEY,
  sensor_SN VARCHAR(50) UNIQUE NOT NULL,
  device_ID VARCHAR(64),
  type VARCHAR(50),
  location VARCHAR(50)
);
```

**Fields:**
- `id`: Unique sensor identifier
- `sensor_SN`: Sensor serial number (unique)
- `device_ID`: Device identifier (links to device_status table)
- `type`: Sensor type (e.g., DS18B20)
- `location`: Physical location of sensor

### Sensor Data Table
Time-series data for temperature readings (hypertable with 2-day compression):

```sql
CREATE TABLE sensor_data (
  time TIMESTAMPTZ NOT NULL,
  sensor_id INTEGER NOT NULL,
  temperature DOUBLE PRECISION,
  cpu DOUBLE PRECISION,
  FOREIGN KEY (sensor_id) REFERENCES sensors (id)
);

-- Converted to hypertable with compression
SELECT create_hypertable('sensor_data', 'time');
ALTER TABLE sensor_data SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'sensor_id',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('sensor_data', INTERVAL '2 days');
```

**Features:**
- Time-series optimized with hypertable partitioning
- Automatic compression of data older than 2 days
- Foreign key constraint to sensors table

### Device Status Table
Stores device status messages and metadata (hypertable with 2-day compression):

```sql
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

-- Converted to hypertable with compression
SELECT create_hypertable('device_status', 'time');
ALTER TABLE device_status SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'device_ID',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('device_status', INTERVAL '2 days');

-- Index for device_ID queries
CREATE INDEX device_status_device_id_idx ON device_status (device_ID, time DESC);
```

**Fields:**
- `time`: Timestamp of status message
- `device_ID`: Device identifier
- `event_category`: Type of status event
- `message_counter`: Sequential message counter
- `git_*`: Software version information
- `num_temp_sensors`, `num_flow_sensors`: Sensor counts
- `is_camera_enabled`: Camera status
- `status_data`: Flexible JSONB field for additional status attributes

**Features:**
- Time-series optimized with hypertable partitioning
- Automatic compression of data older than 2 days
- Indexed on device_ID for efficient device-specific queries
- JSONB column allows flexible status data storage

### Continuous Aggregate Views (Materialized Views)

The database includes continuous aggregate views for efficient temperature analysis:

**10-Minute Aggregates:**
```sql
CREATE MATERIALIZED VIEW sensor_temp_10min AS
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
```

**30-Minute Aggregates:**
```sql
CREATE MATERIALIZED VIEW sensor_temp_30min AS
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
```

**1-Hour Aggregates:**
```sql
CREATE MATERIALIZED VIEW sensor_temp_1h AS
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
```

**Features:**
- Continuous refresh policies for automatic updates
- Indexes on sensor_id and bucket_time for fast queries
- Pre-aggregated data reduces query load for historical analysis
- STDDEV included for anomaly detection

## Query Examples

### Get Latest Temperature for All Sensors

```sql
SELECT s.serial_number, sd.temperature, sd.time
FROM sensor_data sd
JOIN sensors s ON sd.sensor_id = s.id
WHERE sd.time > NOW() - INTERVAL '1 hour'
ORDER BY sd.time DESC;
```

### Get Sensors by Device

```sql
SELECT id, sensor_SN, type, location
FROM sensors
WHERE device_ID = 'device-001'
ORDER BY sensor_SN;
```

### Get Latest Device Status

```sql
SELECT 
  device_ID,
  event_category,
  message_counter,
  git_version,
  num_temp_sensors,
  num_flow_sensors,
  is_camera_enabled,
  status_data,
  time
FROM device_status
WHERE device_ID = 'device-001'
ORDER BY time DESC
LIMIT 10;
```

### Query Device Status with Flexible JSONB Fields

```sql
SELECT 
  device_ID,
  time,
  status_data->>'uptime_seconds' AS uptime,
  status_data->>'memory_usage' AS memory_usage,
  status_data->>'cpu_usage' AS cpu_usage
FROM device_status
WHERE device_ID = 'device-001'
  AND status_data->>'cpu_usage' > '50'
ORDER BY time DESC;
```

### Average Temperature by Sensor (Last 24 Hours)

```sql
SELECT 
  s.serial_number,
  AVG(sd.temperature) as avg_temp,
  MIN(sd.temperature) as min_temp,
  MAX(sd.temperature) as max_temp
FROM sensor_data sd
JOIN sensors s ON sd.sensor_id = s.id
WHERE sd.time > NOW() - INTERVAL '24 hours'
GROUP BY s.id, s.serial_number
ORDER BY avg_temp DESC;
```

### Query 10-Minute Aggregates (Fast Queries)

```sql
SELECT 
  bucket_time,
  sensor_id,
  reading_count,
  avg_temperature,
  min_temperature,
  max_temperature,
  stddev_temperature
FROM sensor_temp_10min
WHERE bucket_time > NOW() - INTERVAL '1 day'
ORDER BY bucket_time DESC, sensor_id;
```

### Query 1-Hour Aggregates for Long-Term Analysis

```sql
SELECT 
  bucket_time,
  sensor_id,
  reading_count,
  avg_temperature,
  min_temperature,
  max_temperature
FROM sensor_temp_1h
WHERE bucket_time > NOW() - INTERVAL '30 days'
  AND sensor_id = 1
ORDER BY bucket_time DESC;
```

### Temperature Anomaly Detection (Using Standard Deviation)

```sql
SELECT 
  bucket_time,
  sensor_id,
  avg_temperature,
  stddev_temperature,
  CASE 
    WHEN stddev_temperature > 2 THEN 'High variability'
    WHEN stddev_temperature > 1 THEN 'Medium variability'
    ELSE 'Stable'
  END as stability
FROM sensor_temp_10min
WHERE bucket_time > NOW() - INTERVAL '1 day'
ORDER BY stddev_temperature DESC;
```

### Hourly Temperature Aggregation (Original Query)

```sql
SELECT
  time_bucket('1 hour', time) AS hour,
  sensor_id,
  AVG(temperature) as avg_temperature,
  MIN(temperature) as min_temperature,
  MAX(temperature) as max_temperature
FROM sensor_data
WHERE time > NOW() - INTERVAL '7 days'
GROUP BY hour, sensor_id
ORDER BY hour DESC;
```

## Python Script Details

The `azure_iot_ingestion.py` script uses the **Azure Event Hubs SDK** for reliable message consumption:

### Key Features

1. **Event Hubs Integration** - Directly consumes messages from IoT Hub's Event Hubs endpoint
2. **Automatic Sensor Registration** - New sensors are registered on first message
3. **Batch Optimization** - Data is batched (100 records per batch) for better database performance
4. **Comprehensive Logging** - All events and errors are logged to both file and console
5. **Error Handling** - Connection failures and malformed messages are gracefully handled
6. **Checkpointing** - Event processing checkpoints are tracked for reliability
7. **Graceful Shutdown** - Remaining data is flushed on disconnect

### Logging

The script logs to:
- **Console**: Real-time visibility during development
- **File**: `iot_ingestion.log` stored inside the container for persistence

Log levels:
- `INFO`: Important events (connections, batch inserts, sensor registration)
- `WARNING`: Recoverable issues (missing fields, skipped records)
- `ERROR`: Failures that need attention (DB errors, connection issues)
- `DEBUG`: Detailed diagnostic information (individual message processing)

Access logs:
```bash
# Real-time console logs
docker logs -f azure-iot-ingestion

# Historical file logs
docker exec azure-iot-ingestion cat iot_ingestion.log

# Tail the file
docker exec azure-iot-ingestion tail -f iot_ingestion.log
```

## Troubleshooting

### Connection String Not Found
Ensure `IOT_HUB_CONNECTION_STRING` is set in `.env` and `docker-compose up -d` was run after updating.

### No Messages Being Consumed
1. Verify IoT devices are sending messages to Azure IoT Hub
2. Check the connection string format in `.env`
3. Verify network connectivity: `docker logs azure-iot-ingestion -f`

### Database Insert Failures
Check logs for SQL errors:
```bash
docker logs azure-iot-ingestion | grep ERROR
```

Verify table schema is correct:
```bash
docker exec timescaledb psql -U postgres -d timescaledb -c "\d sensor_data"
docker exec timescaledb psql -U postgres -d timescaledb -c "\d device_status"
```

Verify continuous aggregates are running:
```bash
docker exec timescaledb psql -U postgres -d timescaledb -c "SELECT view_name, scheduled FROM timescaledb_information.continuous_aggregates;"
```

### Out of Memory
If the container runs out of memory, check batch size in `azure_iot_ingestion.py`:
- Reduce `MAX_BATCH_SIZE` from 100 to a smaller number
- Rebuild the image: `docker compose build`

## Architecture

```
Azure IoT Hub
    ↓ (Event Hubs Protocol)
Event Hubs Consumer
    ↓ (azure-eventhub SDK)
azure-iot-ingestion (Python + Docker)
    ↓ (psycopg2)
TimescaleDB (Hypertables)
    ↓ (SQL queries)
Grafana (Dashboards)
```

## Performance Considerations

- **Hypertable Compression**: Data older than 2 days is automatically compressed
- **Batch Size**: Default 100 records per batch (configurable in script)
- **Checkpointing**: Event offsets are checkpointed after each batch
- **Sensor Cache**: In-memory cache prevents repeated database lookups
- **Connection Pooling**: Single persistent database connection

## Dependencies

See [requirements.txt](requirements.txt) for the complete list. Key dependencies:
- `azure-eventhub>=5.11.6` - Azure Event Hubs SDK
- `psycopg2-binary>=2.9.9` - PostgreSQL driver
- `python-dotenv>=1.0.0` - Environment variable management

## Next Steps

1. **Device Status Monitoring** - Create Grafana dashboards for device health tracking
   - Monitor software versions across devices
   - Track sensor availability (num_temp_sensors, num_flow_sensors)
   - Visualize status message frequency and anomalies

2. **Temperature Trend Analysis** - Use continuous aggregates for historical analysis
   - Query pre-aggregated 10-minute, 30-minute, and 1-hour views
   - Implement sliding window analysis for trend detection
   - Identify temperature patterns by location or sensor type

3. **Status Data Mining** - Leverage JSONB columns for flexible metrics
   - Extract custom metrics from `status_data` JSONB field
   - Create views for device uptime, memory, and CPU tracking
   - Set up alerts for critical status conditions

4. **Enhanced Sensor Metadata** - Add more sensor attributes
   - Calibration information and tolerance ranges
   - Maintenance schedules and last service date
   - Physical installation details (depth, orientation, etc.)

5. **Data Retention Policies** - Configure based on your requirements
   - Current: 2-day compression policy
   - Consider archiving older data to cold storage
   - Set chunk interval based on data volume

6. **Performance Optimization** - Monitor and tune as data grows
   - Review index usage and query execution plans
   - Adjust continuous aggregate refresh intervals
   - Monitor hypertable chunk sizes
