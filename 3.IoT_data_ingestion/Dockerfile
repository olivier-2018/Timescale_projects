FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache && \
    rm -rf /var/lib/apt/lists/*

# Copy all Python application files
COPY *.py .

# Define required environment variables (values provided via docker-compose)
ENV POSTGRES_HOST=$POSTGRES_HOST
ENV POSTGRES_HOST=$POSTGRES_PORT
ENV POSTGRES_USER=$POSTGRES_USER
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD
ENV POSTGRES_DB=$POSTGRES_DB
ENV IOT_HUB_CONNECTION_STRING=$IOT_HUB_CONNECTION_STRING
ENV EVENTHUB_CONSUMER_GROUP=$EVENTHUB_CONSUMER_GROUP
ENV EVENTHUB_IGNORE_PAST=$EVENTHUB_IGNORE_PAST
ENV CHECKPOINT_STORE_CONNECTION_STRING=$CHECKPOINT_STORE_CONNECTION_STRING
ENV CHECKPOINT_STORE_CONTAINER_NAME=$CHECKPOINT_STORE_CONTAINER_NAME
ENV CHECKPOINT_UPDATE_FREQUENCY_MINUTES=$CHECKPOINT_UPDATE_FREQUENCY_MINUTES

# Create non-root user and switch to it
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

# Run the Azure IoT ingestion script
CMD ["python", "azure_iot_ingestion.py"]
