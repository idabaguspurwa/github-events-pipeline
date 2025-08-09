#!/usr/bin/env python3
"""
Quick test script to send test messages to Kafka
"""
import json
import time
from kafka import KafkaProducer

def send_test_messages():
    """Send a few test messages to the github_events topic"""
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:29092'],  # Use the actual port-forwarded port
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: str(k).encode('utf-8') if k else None
    )
    
    # Send 5 test messages
    for i in range(5):
        test_event = {
            "id": f"test-{i}",
            "type": "test_event",
            "created_at": "2025-08-10T00:00:00Z",
            "repo": {
                "id": 12345 + i,
                "name": f"test/repo-{i}"
            },
            "actor": {
                "id": 67890 + i,
                "login": f"test-user-{i}"
            }
        }
        
        # Use repo id as key for partitioning
        key = str(test_event["repo"]["id"])
        
        future = producer.send('github_events', key=key, value=test_event)
        result = future.get(timeout=10)
        print(f"Sent test message {i+1}: {result}")
        time.sleep(1)
    
    producer.flush()
    producer.close()
    print("All test messages sent successfully!")

if __name__ == "__main__":
    send_test_messages()
