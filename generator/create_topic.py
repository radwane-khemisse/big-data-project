import argparse
import os

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


def main():
    parser = argparse.ArgumentParser(description="Create Kafka topic for traffic events")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "traffic-events"))
    parser.add_argument("--partitions", type=int, default=int(os.getenv("KAFKA_PARTITIONS", "3")))
    parser.add_argument("--replication-factor", type=int, default=int(os.getenv("KAFKA_REPLICATION_FACTOR", "1")))
    args = parser.parse_args()

    admin = KafkaAdminClient(bootstrap_servers=args.bootstrap_servers, client_id="traffic-admin")
    topic = NewTopic(
        name=args.topic,
        num_partitions=args.partitions,
        replication_factor=args.replication_factor,
    )

    try:
        admin.create_topics([topic])
        print(f"Topic created: {args.topic}")
    except TopicAlreadyExistsError:
        print(f"Topic already exists: {args.topic}")
    finally:
        admin.close()


if __name__ == "__main__":
    main()
