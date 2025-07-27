# airflow/Dockerfile
FROM ghcr.io/apache/airflow:2.9.2-python3.11

USER airflow

# Copy and install the pinned requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt