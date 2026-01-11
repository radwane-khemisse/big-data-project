import argparse
import json
import os

from kafka import KafkaConsumer


def main():
    parser = argparse.ArgumentParser(description="Simple Kafka consumer for traffic events")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "traffic-events"))
    parser.add_argument("--group-id", default="traffic-consumer-test")
    parser.add_argument("--max-messages", type=int, default=10)
    parser.add_argument("--offset", default="latest", choices=["earliest", "latest"])
    args = parser.parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_servers,
        auto_offset_reset=args.offset,
        group_id=args.group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
    )

    count = 0
    for msg in consumer:
        print(msg.value)
        count += 1
        if count >= args.max_messages:
            break

    consumer.close()


if __name__ == "__main__":
    main()
