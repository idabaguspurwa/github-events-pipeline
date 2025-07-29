from __future__ import annotations
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


# --- Great Expectations Check Function ---
def run_great_expectations():
    import great_expectations as ge
    context = ge.data_context.DataContext("/opt/airflow/great_expectations")
    results = context.run_checkpoint(checkpoint_name="raw_events_checkpoint")
    if not results["success"]:
        raise ValueError("Great Expectations checkpoint failed")


# --- Safely get Snowflake connection ---
try:
    snowflake_conn = BaseHook.get_connection("snowflake_default")
    snowflake_extra = snowflake_conn.extra_dejson
except Exception:
    snowflake_conn = type('obj', (object,), {
        'login': '',
        'password': '',
        'host': '',
        'get_password': lambda self: ''
    })()
    snowflake_extra = {}

# --- Environment Variables for the Kafka consumer ---
k8s_env_vars = {
    "KAFKA_BROKER": "host.minikube.internal:9092",
    "SNOWFLAKE_USER": snowflake_conn.login,
    "SNOWFLAKE_PASSWORD": snowflake_conn.get_password(),
    "SNOWFLAKE_ACCOUNT": snowflake_conn.host,
    "SNOWFLAKE_WAREHOUSE": snowflake_extra.get("warehouse"),
    "SNOWFLAKE_DATABASE": snowflake_extra.get("database"),
    "SNOWFLAKE_ROLE": snowflake_extra.get("role"),
}

# --- Environment Variables for dbt ---
dbt_env = {
    "DBT_USER": snowflake_conn.login,
    "DBT_PASSWORD": snowflake_conn.get_password(),
    "DBT_ACCOUNT": snowflake_conn.host,
    "DBT_WAREHOUSE": snowflake_extra.get("warehouse"),
    "DBT_DATABASE": snowflake_extra.get("database"),
    "DBT_ROLE": snowflake_extra.get("role"),
    "DBT_SCHEMA": snowflake_extra.get("schema"),
}

# --- DAG Definition ---
with DAG(
    dag_id="github_events_elt_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["elt", "github", "final-project"],
) as dag:
    # Task 1: Extract and load to Snowflake
    extract_and_load = KubernetesPodOperator(
        task_id="extract_and_load_to_staging",
        name="kafka-consumer-pod",
        namespace="airflow",
        image="github-consumer:v1",
        image_pull_policy="IfNotPresent",
        cmds=["python", "consumer.py"],
        env_vars=k8s_env_vars,
        config_file="/opt/airflow/kube_config",
        in_cluster=False,
        kubernetes_conn_id=None,
        is_delete_operator_pod=False, # Keep the pod after execution
    )

    # Task 2: Run Great Expectations via Python
    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_great_expectations,
    )

    # Task 3: Run dbt transformations
    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command=(
            "dbt run --profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project/my_dbt_project"
        ),
        env=dbt_env,
    )

    # Task 4: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test --profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project/my_dbt_project"
        ),
        env=dbt_env,
    )

    # Task dependencies
    extract_and_load >> data_quality_check >> dbt_transform >> dbt_test
