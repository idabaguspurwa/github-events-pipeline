# airflow/Dockerfile
FROM apache/airflow:2.9.2-python3.11

USER root
COPY gx_wheels /tmp/gx_wheels
COPY requirements_gx.txt /tmp/
RUN chown -R airflow:root /tmp/gx_wheels /tmp/requirements_gx.txt
USER airflow

# Install dbt first
RUN pip install --no-cache-dir dbt-snowflake

# Now install the latest GX from the local requirements file and wheels folder
RUN pip install --no-cache-dir --no-index --find-links=/tmp/gx_wheels -r /tmp/requirements_gx.txt