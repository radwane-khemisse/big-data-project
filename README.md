# Smart City Traffic - End-to-End Big Data Pipeline

This project implements the full Smart City traffic pipeline described in the assignment PDF: simulated sensors -> Kafka -> HDFS raw -> Spark processing -> analytics Parquet -> Grafana dashboards -> Airflow orchestration.

## Prerequisites
- Docker and Docker Compose
- Python 3.10+ (only if you want to run the generator from the host)

## One-command startup
From the repository root:
```
docker compose -f docker/docker-compose.yml --env-file docker/.env up -d --build
```

## Service UIs
- HDFS NameNode UI: http://localhost:9870
- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081
- Airflow UI: http://localhost:8082
- Grafana UI: http://localhost:3000

## Quick verification commands
- Kafka topic list:
```
docker compose -f docker/docker-compose.yml exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```
- HDFS raw path:
```
docker compose -f docker/docker-compose.yml exec namenode hdfs dfs -ls /data/raw/traffic
```
- Spark jobs folder:
```
docker compose -f docker/docker-compose.yml exec spark-master ls /opt/spark/jobs
```

## How to run the pipeline (manual)
Detailed steps are in the RUNBOOK at the end of this response and in `docs/`.

## Stop and clean volumes
```
docker compose -f docker/docker-compose.yml down -v
```

## Troubleshooting
- Port conflicts: change host ports in `docker/docker-compose.yml` if 9870/8080/8082/3000 are in use.
- Memory: Spark and Kafka need RAM. If containers crash, increase Docker memory to 6-8 GB.
- First Spark run downloads packages for Kafka. Ensure network access is available.

## Documentation
- Architecture and flow: `docs/architecture.md`
- KPI definitions and thresholds: `docs/kpis.md`
- Partitioning and Parquet: `docs/partitioning.md`
- Screenshots checklist: `docs/screenshots_checklist.md`
- Assumptions: `docs/assumptions.md`
