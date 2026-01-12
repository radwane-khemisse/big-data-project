# Commands Runbook (PowerShell)

Run these commands from the project root in PowerShell. Each command is followed by a short explanation of its role.

1) Start the full stack (Kafka, HDFS, Spark, Airflow, Grafana)
```
docker compose -f docker\docker-compose.yml --env-file docker\.env up -d --build
```
Starts all services in the correct network and builds custom images (Airflow) so the pipeline can run end-to-end.

2) Check that containers are up
```
docker compose -f docker\docker-compose.yml ps
```
Confirms each service is running (look for Up/healthy).

3) Wait for Kafka to accept connections (avoids NoBrokersAvailable)
```
docker compose -f docker\docker-compose.yml exec kafka kafka-topics --bootstrap-server kafka:9092 --list
```
If this fails, wait 10–20 seconds and retry until it returns without error.

4) Create the Kafka topic (one time per fresh stack)
```
docker compose -f docker\docker-compose.yml exec airflow-scheduler /home/airflow/.local/bin/python3 /opt/airflow/project/generator/create_topic.py --bootstrap-servers kafka:9092 --topic traffic-events --partitions 1 --replication-factor 1
```
Ensures the Kafka topic exists before producers/consumers start.

5) Trigger the Airflow pipeline (orchestrates generator → streaming ingest → batch KPIs → Postgres)
```
docker compose -f docker\docker-compose.yml exec airflow-scheduler airflow dags unpause smart_city_traffic_pipeline
docker compose -f docker\docker-compose.yml exec airflow-scheduler airflow dags trigger smart_city_traffic_pipeline
```
Unpauses and runs the DAG immediately. The DAG keeps the generator and streaming ingest running, then executes batch KPI jobs every 5 minutes.

6) Verify data is actually being produced (Kafka)
```
docker compose -f docker\docker-compose.yml exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic traffic-events --max-messages 3 --timeout-ms 15000
```
Shows live JSON sensor events if the generator is running.

7) Verify raw data landing in HDFS
```
docker compose -f docker\docker-compose.yml exec namenode bash -c "hdfs dfs -ls /data/raw/traffic"
```
Confirms the streaming job is writing raw events into HDFS partitions.

Note: if streaming fails with an offset error, clear the streaming checkpoint and restart the DAG:
```
docker compose -f docker\docker-compose.yml exec namenode bash -c "hdfs dfs -rm -r -f /data/checkpoints/traffic_raw"
docker compose -f docker\docker-compose.yml exec airflow-scheduler airflow dags trigger smart_city_traffic_pipeline
```
This resets the Kafka offsets tracked by Spark.

8) Verify analytics outputs in HDFS (Parquet)
```
docker compose -f docker\docker-compose.yml exec namenode bash -c "hdfs dfs -ls /data/analytics/traffic"
```
Confirms batch processing created analytics tables for Grafana.

9) Verify KPI tables loaded to Postgres (Grafana datasource)
```
docker compose -f docker\docker-compose.yml exec postgres psql -U airflow -d traffic -c "SELECT 'kpi_traffic_by_zone' AS table, COUNT(*) FROM kpi_traffic_by_zone UNION ALL SELECT 'kpi_speed_by_road', COUNT(*) FROM kpi_speed_by_road UNION ALL SELECT 'kpi_congestion_by_zone', COUNT(*) FROM kpi_congestion_by_zone UNION ALL SELECT 'kpi_critical_zones', COUNT(*) FROM kpi_critical_zones;"
```
Counts should increase after each 5‑minute DAG run.

10) Open Grafana and view dashboards
```
Start-Process "http://localhost:3000"
```
Open the Grafana UI (login admin/admin). Set time range to “Last 30 minutes” and refresh; panels update every 5 minutes as new KPIs load.

11) Monitor Airflow runs and task status
```
docker compose -f docker\docker-compose.yml exec airflow-scheduler airflow dags list-runs -d smart_city_traffic_pipeline -o table
docker compose -f docker\docker-compose.yml exec airflow-scheduler airflow tasks states-for-dag-run smart_city_traffic_pipeline <RUN_ID>
```
Use this to confirm each DAG run is completing and to spot any task failures quickly.

## Restart (clean sequence)
1) Stop containers (keep data)
```
docker compose -f docker\docker-compose.yml down
```
Stops services but keeps volumes.

2) Start stack again
```
docker compose -f docker\docker-compose.yml --env-file docker\.env up -d --build
```
Brings all services back up.

3) Wait for Kafka, then create topic
```
docker compose -f docker\docker-compose.yml exec kafka kafka-topics --bootstrap-server kafka:9092 --list
docker compose -f docker\docker-compose.yml exec airflow-scheduler /home/airflow/.local/bin/python3 /opt/airflow/project/generator/create_topic.py --bootstrap-servers kafka:9092 --topic traffic-events --partitions 1 --replication-factor 1
```
Kafka must be ready before topic creation.

4) Trigger the DAG
```
docker compose -f docker\docker-compose.yml exec airflow-scheduler airflow dags unpause smart_city_traffic_pipeline
docker compose -f docker\docker-compose.yml exec airflow-scheduler airflow dags trigger smart_city_traffic_pipeline
```
Starts the full pipeline again.
