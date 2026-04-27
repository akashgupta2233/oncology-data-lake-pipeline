# Use Python 3.12 slim image for minimal footprint
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Install system dependencies if needed (none required for this pipeline)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better layer caching
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY pipeline/ ./pipeline/
COPY data/ ./data/

# Create output directories (will be mounted as volumes)
RUN mkdir -p datalake/raw datalake/refined datalake/consumption/plots

# Set Python to run in unbuffered mode for real-time logging
ENV PYTHONUNBUFFERED=1

# Run the pipeline
CMD ["python", "pipeline/main.py"]
