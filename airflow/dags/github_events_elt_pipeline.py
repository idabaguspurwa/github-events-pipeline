from __future__ import annotations
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from kubernetes.client import models as k8s

# --- Great Expectations Check Function ---
def run_great_expectations():
    """
    This function is executed by the PythonOperator to run the GX checkpoint.
    """
    import great_expectations as ge
    # The path is now inside the 'repo' directory where git-sync clones your project
    context = ge.data_context.DataContext("/opt/airflow/dags/repo/great_expectations")
    results = context.run_checkpoint(checkpoint_name="raw_events_checkpoint")
    if not results["success"]:
        raise ValueError("Great Expectations checkpoint failed")

# --- Safely get Snowflake connection ---
# This block runs during DAG parsing to populate the dbt environment variables.
try:
    snowflake_conn = Connection.get_connection_from_db(conn_id="snowflake_default")
    snowflake_extra = snowflake_conn.extra_dejson
except Exception:
    # This dummy object prevents the DAG from breaking in the UI if the connection doesn't exist yet
    snowflake_conn = type('obj', (object,), {
        'login': '', 
        'password': '', 
        'host': '', 
        'get_password': lambda self: ''  # Fixed: added 'self' parameter
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
    max_active_runs=1,
) as dag:
    
    # Task 1: Extract and load to Snowflake
    extract_and_load = KubernetesPodOperator(
        task_id="extract_and_load_to_staging",
        name="kafka-consumer-pod",
        namespace="airflow",
        image="github-consumer:v3",  # Updated to v3 with SASL auth
        image_pull_policy="IfNotPresent",
        cmds=["sh", "-c"],
        arguments=["python -u consumer.py 2>&1 | tee /tmp/consumer.log ; exit ${PIPESTATUS[0]}"],
        # Use env_from to reliably mount the manually created 'snowflake-creds' secret
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(
                    name="snowflake-creds",
                )
            )
        ],
        # Pass the correct, simple Kafka broker address and shorter runtime for testing
        env_vars={
             "KAFKA_BROKER": "kafka:9092",
             "RUN_DURATION_SECONDS": "60",  # Reduced to 60 seconds for testing
        },
        in_cluster=True,
        config_file=None,
        kubernetes_conn_id=None,
        # Keep failed pods for debugging. Change to True in production.
        is_delete_operator_pod=False,
        # Add resource limits and requests
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "128Mi", "cpu": "100m"},
            limits={"memory": "512Mi", "cpu": "500m"}
        ),
        # Add startup and liveness probe configuration
        startup_timeout_seconds=120,
        get_logs=True,
        # Add DNS configuration to avoid name resolution issues
        dns_policy="ClusterFirst",
        # Add specific logging configuration
        log_events_on_failure=True,
        # Add timeout configurations
        task_timeout=timedelta(minutes=10),
        # Ensure the pod stays around for log collection
        is_delete_operator_pod=False,
        # Enable log collection from the pod
        get_logs=True,
        # Set log fetch timeout
        startup_timeout_seconds=300, 
    )

    # Task 2: Run Great Expectations via Python
    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_great_expectations,
    )

    # Task 3: Run dbt transformations, using the correct git-sync path
    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command=(
            "dbt run --profiles-dir /opt/airflow/dags/repo/dbt_project "
            "--project-dir /opt/airflow/dags/repo/dbt_project/my_dbt_project"
        ),
        env=dbt_env,
    )

    # Task 4: Run dbt tests, using the correct git-sync path
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
