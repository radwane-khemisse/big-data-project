import argparse
import json
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer

ROAD_TYPES = ["autoroute", "avenue", "rue"]
SPEED_LIMITS = {
    "autoroute": 110.0,
    "avenue": 60.0,
    "rue": 30.0,
}
CAPACITY = {
    "autoroute": 120,
    "avenue": 80,
    "rue": 40,
}


def traffic_factor_by_hour(hour):
    if 7 <= hour < 9 or 17 <= hour < 19:
        return 1.4
    if 9 <= hour < 16:
        return 1.1
    return 0.7


def build_sensors(sensor_count, road_count, zones, seed):
    random.seed(seed)
    roads = []
    for i in range(road_count):
        road_type = random.choice(ROAD_TYPES)
        roads.append({
            "road_id": f"road_{i:03d}",
            "road_type": road_type,
            "zone": random.choice(zones),
        })

    sensors = []
    for i in range(sensor_count):
        road = random.choice(roads)
        sensors.append({
            "sensor_id": f"sensor_{i:04d}",
            "road_id": road["road_id"],
            "road_type": road["road_type"],
            "zone": road["zone"],
        })
    return sensors


def generate_event(sensor, rng):
    now = datetime.utcnow()
    hour = now.hour
    factor = traffic_factor_by_hour(hour)

    capacity = CAPACITY[sensor["road_type"]]
    base = capacity * factor * rng.uniform(0.6, 1.2)
    vehicle_count = max(0, int(rng.gauss(base, base * 0.1)))

    occupancy_rate = (vehicle_count / capacity) * 100.0
    occupancy_rate += rng.uniform(-5.0, 5.0)
    occupancy_rate = max(0.0, min(100.0, occupancy_rate))

    speed_limit = SPEED_LIMITS[sensor["road_type"]]
    speed_drop = occupancy_rate / 140.0
    average_speed = speed_limit * (1.0 - speed_drop) + rng.uniform(-5.0, 5.0)
    average_speed = max(5.0, min(speed_limit, average_speed))

    event = {
        "sensor_id": sensor["sensor_id"],
        "road_id": sensor["road_id"],
        "road_type": sensor["road_type"],
        "zone": sensor["zone"],
        "vehicle_count": int(vehicle_count),
        "average_speed": float(round(average_speed, 2)),
        "occupancy_rate": float(round(occupancy_rate, 2)),
        "event_time": now.strftime("%Y-%m-%d %H:%M:%S"),
    }
    return event


def build_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks=1,
    )


def main():
    parser = argparse.ArgumentParser(description="Traffic sensor simulator and Kafka producer")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "traffic-events"))
    parser.add_argument("--sensors", type=int, default=50)
    parser.add_argument("--roads", type=int, default=20)
    parser.add_argument("--zones", default="north,south,center,east,west")
    parser.add_argument("--rate", type=float, default=20.0, help="events per second")
    parser.add_argument("--sleep-seconds", type=float, default=None, help="fixed sleep between events")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--duration-seconds", type=int, default=0, help="0 means run forever")
    parser.add_argument("--max-events", type=int, default=0, help="0 means no limit")
    parser.add_argument("--stdout-only", action="store_true")

    args = parser.parse_args()

    zones = [z.strip() for z in args.zones.split(",") if z.strip()]
    sensors = build_sensors(args.sensors, args.roads, zones, args.seed)
    rng = random.Random(args.seed)

    producer = None
    if not args.stdout_only:
        producer = build_producer(args.bootstrap_servers)

    if args.sleep_seconds is not None:
        sleep_interval = max(0.0, args.sleep_seconds)
    else:
        sleep_interval = 1.0 / args.rate if args.rate > 0 else 0.0
    event_count = 0
    start_time = time.time()
    next_emit = start_time

    while True:
        if args.duration_seconds and (time.time() - start_time) >= args.duration_seconds:
            break
        if args.max_events and event_count >= args.max_events:
            break

        sensor = rng.choice(sensors)
        event = generate_event(sensor, rng)
        if args.stdout_only:
            print(json.dumps(event))
        else:
            producer.send(args.topic, value=event)
        event_count += 1

        if sleep_interval > 0:
            next_emit += sleep_interval
            sleep_for = max(0.0, next_emit - time.time())
            time.sleep(sleep_for)

    if producer:
        producer.flush()
        producer.close()

    print(f"Generated {event_count} events")


if __name__ == "__main__":
    main()
