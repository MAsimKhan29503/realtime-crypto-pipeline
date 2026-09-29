"""
Throwaway script to verify Python can talk to our Dockerized Kafka.
Not the real pipeline yet -- just proving the connection works end to end.
"""
import json
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# localhost:9092 -- the HOST-facing listener we exposed in docker-compose.yml.
# This is different from the 29092 port other containers use to reach Kafka.
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

TOPIC = "test-topic"

print(f"Sending 10 test messages to Kafka topic '{TOPIC}'...")

for i in range(10):
    message = {
        "hello": "world",
        "sequence": i,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    producer.send(TOPIC, value=message)
    print(f"  sent: {message}")
    time.sleep(1)

producer.flush()  # make sure everything is actually sent before the script exits
producer.close()

print("Done. Check the Kafka UI at http://localhost:8080 -> Topics -> test-topic")