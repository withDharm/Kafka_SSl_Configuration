
from kafka import KafkaConsumer
import json

KAFKA_SERVER = "localhost:9093"
CA_CERT = r"C:\dharm\files\certs\ca.crt"
TOPIC = "test-topic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    security_protocol="SSL",
    ssl_cafile=CA_CERT,
    group_id="python-ssl-consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)


print("Consumer started!", flush=True)
print("Waiting for Wikimedia events...\n", flush=True)

try:
    for message in consumer:
        event = message.value

        print("-" * 60)
        print("Title     :", event.get("title"))
        print("User      :", event.get("user"))
        print("Event type:", event.get("type"))
        print("Wiki      :", event.get("wiki"))
        print("Timestamp :", event.get("timestamp"))
        print("Partition :", message.partition)
        print("Offset    :", message.offset)
        print(flush=True)

except KeyboardInterrupt:
    print("\nConsumer stopped.")

finally:
    consumer.close()
