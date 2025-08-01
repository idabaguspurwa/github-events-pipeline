# airflow/Dockerfile

# Use the same stable version of Airflow and Python
FROM apache/airflow:2.8.4-python3.10

# Switch to root user to install system-level build tools
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    && apt-get clean

# Switch back to the airflow user
USER airflow

# Install a "golden set" of ALL Python packages needed for the entire project
RUN pip install --no-cache-dir \
    "apache-airflow-providers-cncf-kubernetes==8.1.0" \
    "dbt-snowflake==1.5.5" \
    "great-expectations==0.17.23" \
    "sqlalchemy<2.0.0" \
    "kafka-python==2.0.2" \
    "snowflake-connector-python"