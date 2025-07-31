from __future__ import annotations
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from kubernetes.client import models as k8s


# --- Great Expectations Check Function ---
def run_great_expectations():
    import great_expectations as ge
    # Note: The path inside the worker pod will be based on where git-sync clones the repo
    context = ge.data_context.DataContext("/opt/airflow/dags/repo/great_expectations")
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
    max_active_runs=1,  # Prevent multiple concurrent runs
    default_args={
        'retries': 2,  # Reduced from 31 to prevent endless loops
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    
    # Task 1: Extract and load to Snowflake
    extract_and_load = KubernetesPodOperator(
        execution_timeout=timedelta(minutes=30),
        get_logs=True,  # Changed to True for better debugging
        startup_timeout_seconds=600,  # Increased from 300 to give more time
        task_id="extract_and_load_to_staging",
        name="kafka-consumer-pod",
        namespace="airflow",
        image="github-consumer:v1",
        image_pull_policy="IfNotPresent",
        cmds=["python", "consumer.py"],
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(
                    name="snowflake-creds",  # Using your original secret name
                )
            )
        ],
        env_vars={
            # Multiple bootstrap servers for better reliability
            "KAFKA_BROKER": "kafka-controller-0.kafka-controller-headless:9092,kafka-controller-1.kafka-controller-headless:9092,kafka-controller-2.kafka-controller-headless:9092",
        },
        # Add resource limits to prevent resource issues
        resources=k8s.V1ResourceRequirements(
            requests={
                "cpu": "250m",
                "memory": "512Mi"
            },
            limits={
                "cpu": "500m", 
                "memory": "1Gi"
            }
        ),
        # Add health check
        container_logs=True,
        in_cluster=True,
        config_file=None,
        kubernetes_conn_id=None,
        is_delete_operator_pod=True,  # Changed to True for cleanup
        # Add DNS configuration for better service discovery
        dns_policy="ClusterFirst",
        # Add restart policy
        restart_policy="Never",
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
            "dbt run --profiles-dir /opt/airflow/dags/repo/dbt_project "
            "--project-dir /opt/airflow/dags/repo/dbt_project/my_dbt_project"
        ),
        env=dbt_env,
    )

    # Task 4: Run dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "dbt test --profiles-dir /opt/airflow/dags/repo/dbt_project "
            "--project-dir /opt/airflow/dags/repo/dbt_project/my_dbt_project"
        ),
        env=dbt_env,
    )

    # Task dependencies
    extract_and_load >> data_quality_check >> dbt_transform >> dbt_test
