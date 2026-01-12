# Notes

## Event schema (JSON)
{
  "sensor_id": "string",
  "road_id": "string",
  "road_type": "autoroute|avenue|rue",
  "zone": "string",
  "vehicle_count": 0,
  "average_speed": 0.0,
  "occupancy_rate": 0.0,
  "event_time": "YYYY-MM-DD HH:MM:SS"
}

## Kafka ingestion
- Topic: traffic-events
- Partitions: 3
- Producer frequency: 20 events/sec by default (configurable)
- Volume: thousands of events per hour at default rate

## HDFS paths
- Raw: /data/raw/traffic
- Processed: /data/processed/traffic
- Analytics: /data/analytics/traffic

## Grafana
- Datasource: Postgres database traffic

## Batch window
- KPI batch job defaults to last 30 minutes of data for lighter local runs.
- Control with `TRAFFIC_LOOKBACK_MINUTES` in `docker/.env`.
- DAG runs every 5 minutes by default (`TRAFFIC_DAG_SCHEDULE`).

## Low-resource defaults
- Generator sleeps 2 seconds between events (`TRAFFIC_GENERATOR_SLEEP_SECONDS`).
- Spark worker limited to 2 cores and 1g memory.
