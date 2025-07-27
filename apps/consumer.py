# apps/consumer.py
import os
import json
from kafka import KafkaConsumer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = "github_events"

print("--- Starting Consumer ---")
print(f"KAFKA_BROKER: {KAFKA_BROKER}")
print(f"KAFKA_TOPIC: {KAFKA_TOPIC}")

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest', # Start from the beginning of the topic
        group_id='github-event-loader-group', # Consumer group ID
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        api_version=(2, 0, 2)
    )

    print("Consumer is listening...")
    for message in consumer:
        # In the future, this is where we'll load data to Snowflake
        print(f"Received message: Partition={message.partition}, Offset={message.offset}, Key={message.key}")
        print(json.dumps(message.value, indent=2))
        print("-" * 20)

if __name__ == "__main__":
    main()