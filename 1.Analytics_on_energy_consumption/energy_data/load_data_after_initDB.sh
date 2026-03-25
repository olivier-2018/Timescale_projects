#!/bin/sh
set -e

DB_USER="${POSTGRES_USER}"
DB_NAME="${POSTGRES_DB}"

echo "### Waiting for TimescaleDB to be ready..."
until docker exec timescaledb pg_isready -U "$DB_USER" -d "$DB_NAME"; do
  echo "#### sleep 5s"
  sleep 5
done

echo "### TimescaleDB is ready."

# Check if data already loaded
ROWCOUNT=$(docker exec timescaledb psql -U "$DB_USER" -d "$DB_NAME" -t -c "SELECT COUNT(*) FROM metrics;" | tr -d '[:space:]')

if [ "$ROWCOUNT" != "0" ]; then
  echo "### Data already loaded ($ROWCOUNT rows). Skipping."
  exit 0
fi

echo "### Data not found. Proceeding with load."

echo "### Copying compressed file into TimescaleDB container..."
docker cp /energy_data/metrics.csv.gz timescaledb:/tmp/data.csv.gz

echo "### Decompressing inside TimescaleDB container..."
docker exec timescaledb sh -c "gunzip -f /tmp/data.csv.gz"

echo "### Loading data into metrics table..."
docker exec timescaledb psql -U "$DB_USER" -d "$DB_NAME" -c "\copy metrics FROM '/tmp/data.csv' CSV HEADER"

echo "### Data load complete."
