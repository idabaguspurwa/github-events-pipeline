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


def get_kafka_consumer():
    while True:
        try:
            # Handle both single and multiple bootstrap servers
            if ',' in KAFKA_BROKER:
                # Multiple servers: split by comma and strip whitespace
                bootstrap_servers = [broker.strip() for broker in KAFKA_BROKER.split(',')]
            else:
                # Single server: use as-is
                bootstrap_servers = [KAFKA_BROKER]
            
            print(f"Connecting to Kafka brokers: {bootstrap_servers}")
            
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                group_id='github-event-loader-group',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                api_version=(2, 0, 2),
                # Add connection timeout and retry settings
                request_timeout_ms=60000,
                connections_max_idle_ms=540000,
                reconnect_backoff_ms=50,
                reconnect_backoff_max_ms=1000,
            )
            print("Successfully connected to Kafka.")
            return consumer
        except Exception as e:
            print(f"Could not connect to Kafka: {e}. Retrying...")
            time.sleep(5)


def main():
    print("--- Starting Consumer ---")
    
    # Validate Kafka broker configuration
    if not KAFKA_BROKER:
        raise ValueError("KAFKA_BROKER environment variable is required")
    
    # Check for missing Snowflake credentials
    for key, value in SNOWFLAKE_CONFIG.items():
        if not value and key != "schema": # schema is hardcoded
             raise ValueError(f"Missing required environment variable for Snowflake connection: {key.upper()}")

    consumer = get_kafka_consumer()
    
    # Add connection retry logic for Snowflake
    conn = None
    max_retries = 3
    for attempt in range(max_retries):
        try:
            conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            print("Successfully connected to Snowflake.")
            break
        except Exception as e:
            print(f"Snowflake connection attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(5)
    
    batch = []
    batch_size = 100
    start_time = time.time()

    print(f"Consumer will run for {RUN_DURATION_SECONDS} seconds.")

    try:
        while time.time() - start_time < RUN_DURATION_SECONDS:
            records = consumer.poll(timeout_ms=1000, max_records=batch_size)
            
            if not records:
                continue

            for topic_partition, messages in records.items():
                for message in messages:
                    batch.append(message.value)
            
            if batch:
                # Use cursor in a more robust way
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        "INSERT INTO RAW_EVENTS (V) SELECT PARSE_JSON(column1) FROM VALUES " + ", ".join(["(%s)"] * len(batch)),
                        [json.dumps(rec) for rec in batch]
                    )
                    print(f"Inserted {len(batch)} records into Snowflake.")
                    batch = []
                except Exception as e:
                    print(f"Error inserting batch to Snowflake: {e}")
                    # You might want to handle this differently based on your requirements
                finally:
                    cursor.close()
        
        # Insert any remaining records in the batch
        if batch:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "INSERT INTO RAW_EVENTS (V) SELECT PARSE_JSON(column1) FROM VALUES " + ", ".join(["(%s)"] * len(batch)),
                    [json.dumps(rec) for rec in batch]
                )
                print(f"Inserted final batch of {len(batch)} records into Snowflake.")
            except Exception as e:
                print(f"Error inserting final batch to Snowflake: {e}")
            finally:
                cursor.close()
        
    except KeyboardInterrupt:
        print("Consumer interrupted by user.")
    except Exception as e:
        print(f"Consumer error: {e}")
    finally:
        print("Run duration reached. Finishing up.")
        if conn:
            conn.close()
        consumer.close()
        print("--- Consumer Finished ---")


if __name__ == "__main__":
    main()
