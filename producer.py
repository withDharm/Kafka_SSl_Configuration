from kafka import KafkaProducer
import json
import requests

KAFKA_SERVER = "localhost:9093"
CA_CERT = r"C:\dharm\files\certs\ca.crt"
STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
TOPIC = "test-topic"

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    security_protocol="SSL",
    ssl_cafile=CA_CERT,
    value_serializer=lambda value: json.dumps(
        value, ensure_ascii=False
    ).encode("utf-8"),
    acks="all"
)

print("Connecting to Wikimedia...", flush=True)

try:
    with requests.get(
        STREAM_URL,
        headers={
            "User-Agent": "WikiKafkaLearning/1.0 (learning project)",
            "Accept": "text/event-stream"
        },
        stream=True,
        timeout=(15, 60)
    ) as response:

        print("HTTP Status:", response.status_code, flush=True)
        response.raise_for_status()

        print("Connected! Sending events to Kafka...", flush=True)

        data_lines = []

        for line in response.iter_lines(decode_unicode=True):

            if not line:
                if data_lines:
                    raw_data = "\n".join(data_lines)
                    data_lines.clear()

                    try:
                        event = json.loads(raw_data)

                        future = producer.send(TOPIC, value=event)
                        metadata = future.get(timeout=30)

                        print(
                            f"KAFKA SENT | Topic={metadata.topic} "
                            f"| Partition={metadata.partition} "
                            f"| Offset={metadata.offset} "
                            f"| Title={event.get('title', 'N/A')}",
                            flush=True
                        )

                    except json.JSONDecodeError as exc:
                        print("JSON error:", exc, flush=True)

                    except Exception as exc:
                        print("Kafka error:", repr(exc), flush=True)

                continue

            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip())

except KeyboardInterrupt:
    print("\nProducer stopped.", flush=True)

except requests.RequestException as exc:
    print("Wikimedia connection error:", repr(exc), flush=True)

finally:
    producer.flush()
    producer.close()
    print("Producer closed.", flush=True)
