# apps/producer.py
import os
import time
import json
import requests
from kafka import KafkaProducer
from prometheus_client import start_http_server, Counter, Gauge

# Prometheus Metrics
EVENTS_PRODUCED = Counter('events_produced_total', 'Total number of events produced to Kafka')
LATEST_EVENT_TIMESTAMP = Gauge('latest_event_timestamp_seconds', 'Timestamp of the latest event produced')
PRODUCER_UP = Gauge('producer_up', 'Is the producer running', ['producer_name'])

# Configurations
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "github_events"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
API_URL = "https://api.github.com/events"

print("--- Starting Producer ---")
print(f"KAFKA_BROKER: {KAFKA_BROKER}")
print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")

if not GITHUB_TOKEN:
    raise ValueError("GITHUB_TOKEN environment variable not set!")

# Connect to Kafka and return a producer instance
def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 0, 2)
            )
            print("Successfully connected to Kafka")
            return producer
        except Exception as e:
            print(f"Could not connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# Main loop to fetch and produce events
def main():
    producer = get_kafka_producer()
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    last_etag = None

    # Start Prometheus Metrics Server
    start_http_server(8000)
    PRODUCER_UP.labels('github-producer').set(1)

    while True:
        try:
            if last_etag:
                headers['If-None-Match'] = last_etag
            
            response = requests.get(API_URL, headers=headers, timeout=10)

            # Updated ETag to avoid fetching the same data
            if 'ETag' in response.headers:
                last_etag = response.headers['ETag']
            
            # If status is 304, data has not changed
            if response.status_code == 304:
                print("No new events. Waiting...")
                time.sleep(60)
                continue

            response.raise_for_status() # Raise an exception for bad status codes
            events = response.json()

            if not events:
                continue

            print(f"Fetched {len(events)} new events")

            for event in events:
                # Use repository ID as the message key for partitioning
                key = str(event['repo']['id']).encode('utf-8')
                producer.send(KAFKA_TOPIC, key=key, value=event)
                EVENTS_PRODUCED.inc()
                LATEST_EVENT_TIMESTAMP.set_to_current_time()
            
            producer.flush()
            print(f"Successfully produced {len(events)} events to Kafka")
            time.sleep(5)
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching from GitHub API: {e}")
            time.sleep(30)
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            producer.close()
            time.sleep(10)
            producer = get_kafka_producer() # Reconnect on failure

if __name__ == "__main__":
    main()