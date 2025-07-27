# airflow/Dockerfile
FROM apache/airflow:2.9.2-python3.11

USER airflow

# Install all necessary python packages
RUN pip install --no-cache-dir \
    dbt-snowflake \
    "great-expectations[snowflake]" \
    apache-airflow-providers-cncf-kubernetes