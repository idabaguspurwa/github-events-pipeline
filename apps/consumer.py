# apps/consumer.py
import os
import json
import time
from kafka import KafkaConsumer
import snowflake.connector

# --- Config ---
KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
KAFKA_TOPIC = "github_events"

# CORRECTED: Use the standard environment variables provided by the Airflow connection secret
SNOWFLAKE_CONFIG = {
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
    "database": os.environ.get("SNOWFLAKE_DATABASE"),
    "schema": "RAW",
    "role": os.environ.get("SNOWFLAKE_ROLE"),
}
RUN_DURATION_SECONDS = 300 # 5 minutes

# (The get_kafka_consumer function remains the same)
def get_kafka_consumer():
    while True:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                auto_offset_reset='earliest',
                group_id='github-event-loader-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                api_version=(2, 0, 2),
            )
            print("Successfully connected to Kafka.")
            return consumer
        except Exception as e:
            print(f"Could not connect to Kafka: {e}. Retrying...")
            time.sleep(5)

def main():
    print("--- Starting Consumer ---")
    
    # Check for missing Snowflake credentials
    for key, value in SNOWFLAKE_CONFIG.items():
        if not value and key != "schema": # schema is hardcoded
             raise ValueError(f"Missing required environment variable for Snowflake connection: {key.upper()}")

    consumer = get_kafka_consumer()
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print("Successfully connected to Snowflake.")
    
    batch = []
    batch_size = 100
    start_time = time.time()

    print(f"Consumer will run for {RUN_DURATION_SECONDS} seconds.")

    while time.time() - start_time < RUN_DURATION_SECONDS:
        records = consumer.poll(timeout_ms=1000, max_records=batch_size)
        
        if not records:
            continue

        for topic_partition, messages in records.items():
            for message in messages:
                batch.append(message.value)
        
        if batch:
            conn.cursor().execute(
                "INSERT INTO RAW_EVENTS (V) SELECT PARSE_JSON(column1) FROM VALUES " + ", ".join(["(%s)"] * len(batch)),
                [json.dumps(rec) for rec in batch]
            )
            print(f"Inserted {len(batch)} records into Snowflake.")
            batch = []
    
    print("Run duration reached. Finishing up.")
    conn.close()
    consumer.close()
    print("--- Consumer Finished ---")

if __name__ == "__main__":
    main()