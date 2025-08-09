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
API_URL = os.environ.get("GITHUB_API_URL", "https://api.github.com/events")
FETCH_INTERVAL = int(os.environ.get("FETCH_INTERVAL", "30"))
RUN_DURATION = int(os.environ.get("RUN_DURATION_SECONDS", "300"))

print("🚀 --- Starting GitHub Events Producer ---")
print(f"📡 KAFKA_BROKER: {KAFKA_BROKER}")
print(f"📝 KAFKA_TOPIC: {KAFKA_TOPIC}")
print(f"🌐 API_URL: {API_URL}")
print(f"⏱️ FETCH_INTERVAL: {FETCH_INTERVAL} seconds")
print(f"⏰ RUN_DURATION: {RUN_DURATION} seconds")
print(f"🔑 GITHUB_TOKEN: {'✅ Set' if GITHUB_TOKEN else '❌ Not set (will use dummy data)'}")

# Use dummy data if no token provided (for testing)
USE_DUMMY_DATA = not GITHUB_TOKEN or GITHUB_TOKEN == "dummy_token_for_testing"

if USE_DUMMY_DATA:
    print("⚠️ Using dummy data mode for testing (no real GitHub API calls)")
else:
    print("✅ Using real GitHub API with provided token")

# Connect to Kafka and return a producer instance
def get_kafka_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                api_version=(2, 0, 2),
                # Add SASL authentication
                security_protocol='SASL_PLAINTEXT',
                sasl_mechanism='SCRAM-SHA-256',
                sasl_plain_username='user1',
                sasl_plain_password='9WDcJnfRut',
            )
            print("Successfully connected to Kafka")
            return producer
        except Exception as e:
            print(f"Could not connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

# Main loop to fetch and produce events
def generate_dummy_event():
    """Generate a dummy GitHub event for testing"""
    import random
    import datetime
    
    event_types = ['PushEvent', 'IssuesEvent', 'PullRequestEvent', 'WatchEvent', 'ForkEvent']
    
    return {
        'id': str(random.randint(1000000000, 9999999999)),
        'type': random.choice(event_types),
        'actor': {
            'id': random.randint(1000, 9999),
            'login': f'test_user_{random.randint(1, 100)}',
            'display_login': f'test_user_{random.randint(1, 100)}',
        },
        'repo': {
            'id': random.randint(100000, 999999),
            'name': f'test_org/test_repo_{random.randint(1, 50)}',
            'url': f'https://api.github.com/repos/test_org/test_repo_{random.randint(1, 50)}'
        },
        'payload': {},
        'public': True,
        'created_at': datetime.datetime.utcnow().isoformat() + 'Z'
    }

def main():
    producer = get_kafka_producer()
    
    # Start Prometheus Metrics Server
    start_http_server(8000)
    PRODUCER_UP.labels('github-producer').set(1)
    
    print(f"🔄 Starting main producer loop for {RUN_DURATION} seconds...")
    
    start_time = time.time()
    last_etag = None
    event_count = 0
    
    while time.time() - start_time < RUN_DURATION:
        try:
            if USE_DUMMY_DATA:
                # Generate dummy events for testing
                print("🎲 Generating dummy events for testing...")
                events = [generate_dummy_event() for _ in range(5)]  # Generate 5 dummy events
                print(f"📦 Generated {len(events)} dummy events")
                
            else:
                # Real GitHub API calls
                headers = {'Authorization': f'token {GITHUB_TOKEN}'}
                if last_etag:
                    headers['If-None-Match'] = last_etag
                
                print(f"🌐 Fetching events from GitHub API: {API_URL}")
                response = requests.get(API_URL, headers=headers, timeout=10)

                # Updated ETag to avoid fetching the same data
                if 'ETag' in response.headers:
                    last_etag = response.headers['ETag']
                
                # If status is 304, data has not changed
                if response.status_code == 304:
                    print("📭 No new events from API. Waiting...")
                    time.sleep(30)
                    continue

                response.raise_for_status()
                events = response.json()

                if not events:
                    print("📭 No events received from API")
                    time.sleep(30)
                    continue

                print(f"📡 Fetched {len(events)} new events from GitHub API")

            # Produce events to Kafka
            for event in events:
                try:
                    # Use repository ID as the message key for partitioning
                    key = str(event['repo']['id']).encode('utf-8')
                    producer.send(KAFKA_TOPIC, key=key, value=event)
                    EVENTS_PRODUCED.inc()
                    LATEST_EVENT_TIMESTAMP.set_to_current_time()
                    event_count += 1
                except Exception as e:
                    print(f"❌ Error sending event: {e}")
            
            producer.flush()
            print(f"✅ Successfully produced {len(events)} events to Kafka (Total: {event_count})")
            
            # Wait before next fetch
            print(f"⏱️ Waiting {FETCH_INTERVAL} seconds before next fetch...")
            time.sleep(FETCH_INTERVAL)
        
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching from GitHub API: {e}")
            time.sleep(30)
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")
            print("🔄 Attempting to reconnect to Kafka...")
            try:
                producer.close()
            except:
                pass
            time.sleep(10)
            producer = get_kafka_producer()
    
    # Cleanup
    print(f"🏁 Producer run completed. Total events produced: {event_count}")
    PRODUCER_UP.labels('github-producer').set(0)
    producer.close()

if __name__ == "__main__":
    main()