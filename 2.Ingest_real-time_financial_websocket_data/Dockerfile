FROM python:3.11-slim

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache && \
    rm -rf /var/lib/apt/lists/*

# Copy application files
COPY batching_ingestion.py .

# Define environment variables
ENV POSTGRES_HOST=timescaledb
ENV POSTGRES_USER=$POSTGRES_USER
ENV POSTGRES_PASSWORD=$POSTGRES_PASSWORD
ENV POSTGRES_DB=$POSTGRES_DB
ENV TWELVEDATA_API_KEY=$TWELVEDATA_API_KEY

# Create non-root user and switch to it
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

# Run the batching ingestion script
CMD ["python", "batching_ingestion.py"]
