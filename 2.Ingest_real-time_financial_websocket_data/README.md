# Project 2: Real-time financial data ingestion via a websocket

## Overview

A websocket is an open-ended connection to a data source.  
It is very useful for real-time data ingestion and avoid large data overhead to establish the connection to a data store.   

This project automates the setup and deployment of TimescaleDB, grafana and the data ingestion micro-service using a websocket to Twelve-data.  

## Prerequisites

- Python 3.11 or higher
- [uv](https://github.com/astral-sh/uv) package manager
- Docker and Docker Compose

- an account by Twelve-Data: [Twelve-Data](https://twelvedata.com/)

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

### WebSocket connection with Twelve-Data

The python file *websocket_test.py* shows how to establish a websocket connection to the Tewelve-Data datasets and stream events from it.  

The TDClient function from the twelvedata library is used and requires 2 args:
- an on_event function to ingest data
- symbols (for the desired stock market share)

### Batching in memory

A common practice is to implement batching:
- store new records in memory first, until a batch size is reached 
- then, insert all records from memory into the database in one transaction

**Note:** 
- The batch size must be experimented with (for example, 100, 1000, 10000, and so on) to see which one fits the use case best. 
- Using batching is a fairly common pattern when ingesting data into TimescaleDB from Kafka, Kinesis, or websocket connections.

The batching solution is implemented using the Psycopg2 library.  

This ingestion logic for the *on_event* function will:
- Check if the item is a data item, and not websocket metadata.
- Adjust the data so that it fits the database schema, including the data types, and order of columns.
- Add it to the in-memory batch, which is a list in Python.
- If the batch reaches a certain size, insert the data, and reset or empty the list.

### Running batch ingestion in dokcer container

Finally the batch ingestion script is deployed automatically via docker compose.  
A docker image is first build from a python3.11 slim image to run the script.  

Data ingestion can be monitored using the  **batching_ingestion.py** script logs:
```bash
docker logs batching-ingestion -f  
```

## Setup grafana

### Create data source to Timescale

Got to http://localhost:3003  
In grafana, select Connections --> Data Sources --> click add.

### Provisioned data source and dashboards

TimescaleDB data source and grafana dashboards are automatically provisioned from the *grafana_provisioning* folder.

## Acknowledgements:

[TigerData (Timescale)](https://www.tigerdata.com/docs/tutorials/latest/financial-ingest-real-time)

