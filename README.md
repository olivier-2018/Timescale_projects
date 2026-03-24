# TimescaleDB projects


## Overview

This repo showcasts a few projects using Timescale DB features such as:
- hypertable and compression management
- connector to various data sources
- Docker container setup for local deployment
- DB init and data loading automation

Below are the projects committed so far to this repo:
1. Analytics on energy consumption



**Notes:**  
- Each project is independent and can be run in isolation.  
- Avoid running 2 projects at once as the container may interfere with one another.  
- Simply cd in the selected folder and follow the instructions in the README file.  
- Most of the time, a simply *docker compose up* is the only thing to do to get access to grafana.  

## Getting started

### Requirements
All projects assume you have a number of tools installed on your host system.    
All container images will be downloaded automatically as required.  

Run the following cmds to install the required tools:
```bash
# install postgres client
sudo apt install postgresql-client 
```

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create a Python 3.11 environment
uv venv --python 3.11 .venv

# Activate the virtual environment
source .venv/bin/activate  
```

### Environment setup

Prior to running *docker compose up*, run the following:
```bash
cp .env.sample .env
```
then modify the .env with you personal preferences.
 
### Connec to DB
```bash
# if postgres-client installed
psql -p 5432 -h ${POSTGRES_HOST} -U  ${POSTGRES_USER}

# Directly from docker container
docker exec -it timescaleDB psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}
```

### Basic cmd: Create hypertables
```sql
-- Create Table
CREATE TABLE stock_price (
    time TIMESTAMPZ NOT NULL,
    symbol  TEXT NOT NULL,
    price DOUBLE PRECISION NULL,
    day_volume INT NULL
);
-- special Timescale function to convert std table into a HYPER table
SELECT create_hypertable ('stock_price',  'time');  
-- Create Table index
CREATE INDEX ix_symbol_time ON stock_price (symbol  , time DESC);
-- Create Dimension Table for company names
CREATE TABLE company (
    symbol  TEXT NOT NULL,
    name TEXT NOT NULL
);
```

## Acknowledgements:

[TigerData (Timescale)](https://www.tigerdata.com/docs/tutorials/latest)


