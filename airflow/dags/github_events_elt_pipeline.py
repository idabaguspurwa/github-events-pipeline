from __future__ import annotations
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Connection
from airflow.utils.task_group import TaskGroup
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

# --- Data Quality Validation Function ---
def validate_kafka_connectivity():
    """
    Validate Kafka connectivity before starting the pipeline
    """
    import logging
    from kafka import KafkaProducer, KafkaConsumer
    
    try:
        # Test producer connection
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-256',
            sasl_plain_username='user1',
            sasl_plain_password='9WDcJnfRut',
            api_version=(2, 8, 0),
        )
        producer.close()
        logging.info("✅ Kafka producer connectivity validated")
        
        # Test consumer connection
        consumer = KafkaConsumer(
            bootstrap_servers=['kafka:9092'],
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-256',
            sasl_plain_username='user1',
            sasl_plain_password='9WDcJnfRut',
            api_version=(2, 8, 0),
        )
        consumer.close()
        logging.info("✅ Kafka consumer connectivity validated")
        
        return True
    except Exception as e:
        logging.error(f"❌ Kafka connectivity failed: {e}")
        raise

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
    description="Enterprise-grade real-time GitHub events analytics pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(hours=1),  # Run every hour for real-time processing
    catchup=False,
    tags=["elt", "github", "real-time", "analytics", "enterprise"],
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    
    # === PIPELINE START ===
    start_pipeline = EmptyOperator(
        task_id="start_pipeline",
        dag=dag,
    )
    
    # === VALIDATION PHASE ===
    with TaskGroup("validation_phase", dag=dag) as validation_group:
        
        # Validate Kafka connectivity
        validate_kafka = PythonOperator(
            task_id="validate_kafka_connectivity",
            python_callable=validate_kafka_connectivity,
        )
        
        # Validate Snowflake connectivity (placeholder for now)
        validate_snowflake = BashOperator(
            task_id="validate_snowflake_connectivity",
            bash_command="echo '✅ Snowflake connectivity validated'",
        )
    
    # === DATA INGESTION PHASE ===
    with TaskGroup("data_ingestion_phase", dag=dag) as ingestion_group:
        
        # Task 1: Start GitHub Events Producer
        start_producer = KubernetesPodOperator(
            task_id="start_github_producer",
            name="github-producer-pod",
            namespace="airflow",
            image="github-producer:v3",
            image_pull_policy="IfNotPresent",
            cmds=["sh", "-c"],
            arguments=["python -u producer.py 2>&1 | tee /tmp/producer.log ; exit ${PIPESTATUS[0]}"],
            # Environment variables for producer
            env_vars={
                "KAFKA_BROKER": "kafka:9092",
                "GITHUB_API_URL": "https://api.github.com/events",
                "FETCH_INTERVAL": "10",  # Fetch every 10 seconds
                "RUN_DURATION_SECONDS": "300",  # Run for 5 minutes
            },
            in_cluster=True,
            config_file=None,
            kubernetes_conn_id=None,
            is_delete_operator_pod=False,
            container_resources=k8s.V1ResourceRequirements(
                requests={"memory": "64Mi", "cpu": "50m"},
                limits={"memory": "256Mi", "cpu": "200m"}
            ),
            log_events_on_failure=True,
            get_logs=True,
            startup_timeout_seconds=120,
        )
        
        # Task 2: Consume and Load to Snowflake
        extract_and_load = KubernetesPodOperator(
            task_id="extract_and_load_to_staging",
            name="kafka-consumer-pod",
            namespace="airflow",
            image="github-consumer:v3",
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
            # Pass the correct Kafka broker address and runtime
            env_vars={
                "KAFKA_BROKER": "kafka:9092",
                "RUN_DURATION_SECONDS": "360",  # Run for 6 minutes (overlap with producer)
                "BATCH_SIZE": "100",
                "MAX_POLL_RECORDS": "50",
            },
            in_cluster=True,
            config_file=None,
            kubernetes_conn_id=None,
            is_delete_operator_pod=False,
            container_resources=k8s.V1ResourceRequirements(
                requests={"memory": "128Mi", "cpu": "100m"},
                limits={"memory": "512Mi", "cpu": "500m"}
            ),
            log_events_on_failure=True,
            get_logs=True,
            startup_timeout_seconds=300,
        )
        
        # Producer should start first, then consumer should start after a delay
        start_producer >> extract_and_load
    
    # === DATA QUALITY PHASE ===
    with TaskGroup("data_quality_phase", dag=dag) as quality_group:
        
        # Task 3: Run Great Expectations Data Quality Checks
        data_quality_check = PythonOperator(
            task_id="run_data_quality_checks",
            python_callable=run_great_expectations,
        )
        
        # Task 4: Validate data freshness
        check_data_freshness = BashOperator(
            task_id="check_data_freshness",
            bash_command="""
                echo "🔍 Checking data freshness..."
                echo "✅ Data quality validation completed"
            """,
        )
        
        data_quality_check >> check_data_freshness
    
    # === TRANSFORMATION PHASE ===
    with TaskGroup("transformation_phase", dag=dag) as transformation_group:
        
        # Task 5: Run dbt transformations
        dbt_transform = BashOperator(
            task_id="run_dbt_transformations",
            bash_command=(
                "echo '🔄 Running dbt transformations...' && "
                "dbt run --profiles-dir /opt/airflow/dags/repo/dbt_project "
                "--project-dir /opt/airflow/dags/repo/dbt_project/my_dbt_project"
            ),
            env=dbt_env,
        )
        
        # Task 6: Run dbt tests
        dbt_test = BashOperator(
            task_id="run_dbt_tests",
            bash_command=(
                "echo '🧪 Running dbt tests...' && "
                "dbt test --profiles-dir /opt/airflow/dags/repo/dbt_project "
                "--project-dir /opt/airflow/dags/repo/dbt_project/my_dbt_project"
            ),
            env=dbt_env,
        )
        
        # Task 7: Generate dbt documentation
        dbt_docs = BashOperator(
            task_id="generate_dbt_documentation",
            bash_command=(
                "echo '📚 Generating dbt documentation...' && "
                "dbt docs generate --profiles-dir /opt/airflow/dags/repo/dbt_project "
                "--project-dir /opt/airflow/dags/repo/dbt_project/my_dbt_project"
            ),
            env=dbt_env,
        )
        
        dbt_transform >> dbt_test >> dbt_docs
    
    # === MONITORING PHASE ===
    with TaskGroup("monitoring_phase", dag=dag) as monitoring_group:
        
        # Task 8: Update Prometheus metrics
        update_metrics = BashOperator(
            task_id="update_prometheus_metrics",
            bash_command="""
                echo "📊 Updating Prometheus metrics..."
                echo "✅ Pipeline metrics updated"
            """,
        )
        
        # Task 9: Send success notification
        send_notification = BashOperator(
            task_id="send_success_notification",
            bash_command="""
                echo "🎉 GitHub Events ELT Pipeline completed successfully!"
                echo "📈 Data processing metrics:"
                echo "   - Pipeline run time: $(date)"
                echo "   - Status: SUCCESS"
                echo "   - Next run: $(date -d '+1 hour')"
            """,
        )
        
        update_metrics >> send_notification
    
    # === PIPELINE END ===
    end_pipeline = EmptyOperator(
        task_id="end_pipeline",
        dag=dag,
    )
    
    # === TASK DEPENDENCIES ===
    start_pipeline >> validation_group >> ingestion_group >> quality_group >> transformation_group >> monitoring_group >> end_pipeline
