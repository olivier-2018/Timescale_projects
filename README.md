# TimescaleDB tutorials

## Requirements
sudo apt install postgresql-client 

## Getting started

git lfs install 
git lfs track "*.csv"
git lfs track "*.gz"
git lfs track "*.bin"


## Basic commands

### Connec to DB
```bash
# if postgres-client installed
psql -p 5432 -h ${POSTGRES_HOST} -U  ${POSTGRES_USER}
```bash

```bash
# Directly from docker container
docker exec -it timescaleDB psql -U ${POSTGRES_USER} -d ${POSTGRES_DB}
```

### Create hypertables
```sql
CREATE TABLE stock_price (
    time TIMESTAMPZ NOT NULL,
    symbol  TEXT NOT NULL,
    price DOUBLE PRECISION NULL,
    day_volume INT NULL
);

SELECT create_hypertable ('stock_price',  'time');  -- special Timescale function to convert std table into a HYPER table

CREATE INDEX ix_symbol_time ON stock_price (symbol  , time DESC);

CREATE TABLE company (
    symbol  TEXT NOT NULL,
    name TEXT NOT NULL
);
```

## Acknowledgements:

[TigerData (Timescale)](https://www.tigerdata.com/docs/tutorials/latest/financial-ingest-real-time/financial-ingest-dataset)


