# airflow/Dockerfile

# Use a stable, slightly older version of Airflow and Python
FROM apache/airflow:2.8.4-python3.10

# Switch to root user to install system-level build tools like cmake
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    && apt-get clean

# Switch back to the airflow user for package installations
USER airflow

# Copy and install the pinned requirements file
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt