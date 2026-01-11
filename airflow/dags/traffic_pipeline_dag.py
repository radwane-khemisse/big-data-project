import os
import socket
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def wait_for_service(host, port, timeout=120):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=5):
                return
        except OSError:
            time.sleep(3)
    raise RuntimeError(f"Service not ready: {host}:{port}")


def wait_for_stack():
    wait_for_service("kafka", 9092)
    wait_for_service("namenode", 8020)
    wait_for_service("spark-master", 7077)
    wait_for_service("postgres", 5432)


def validate_kpis():
    import psycopg2

    conn = psycopg2.connect(
        host="postgres",
        port=5432,
        dbname=os.getenv("TRAFFIC_DB", "traffic"),
        user=os.getenv("TRAFFIC_DB_USER", "traffic_user"),
        password=os.getenv("TRAFFIC_DB_PASSWORD", "traffic_pass"),
    )
    cur = conn.cursor()
    for table in [
        "kpi_traffic_by_zone",
        "kpi_speed_by_road",
        "kpi_congestion_by_zone",
        "kpi_critical_zones",
    ]:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        if count == 0 and table != "kpi_critical_zones":
            raise RuntimeError(f"Validation failed: {table} is empty")
    cur.close()
    conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="smart_city_traffic_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["smart-city", "traffic"],
) as dag:
    wait_stack = PythonOperator(task_id="wait_for_services", python_callable=wait_for_stack)

    create_topic = BashOperator(
        task_id="create_kafka_topic",
        bash_command=(
            "/usr/bin/python3 /opt/airflow/project/generator/create_topic.py "
            "--bootstrap-servers $KAFKA_BOOTSTRAP_SERVERS "
            "--topic $KAFKA_TOPIC "
            "--partitions $KAFKA_PARTITIONS "
            "--replication-factor $KAFKA_REPLICATION_FACTOR"
        ),
        env={
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "traffic-events"),
            "KAFKA_PARTITIONS": os.getenv("KAFKA_PARTITIONS", "3"),
            "KAFKA_REPLICATION_FACTOR": os.getenv("KAFKA_REPLICATION_FACTOR", "1"),
        },
    )

    start_generator = BashOperator(
        task_id="start_generator",
        bash_command=(
            "bash -c \"mkdir -p /opt/airflow/logs; "
            "if ps -ef | grep -v grep | grep -q 'traffic_generator.py'; then "
            "echo 'Generator already running'; "
            "else "
            "nohup /usr/bin/python3 /opt/airflow/project/generator/traffic_generator.py "
            "--bootstrap-servers $KAFKA_BOOTSTRAP_SERVERS "
            "--topic $KAFKA_TOPIC "
            "--rate 1 "
            "> /opt/airflow/logs/traffic_generator.log 2>&1 & "
            "echo $! > /opt/airflow/logs/traffic_generator.pid; "
            "fi\""
        ),
        env={
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "traffic-events"),
        },
    )

    start_streaming = BashOperator(
        task_id="start_streaming_ingest",
        bash_command=(
            "bash -c \"mkdir -p /tmp/ivy /opt/airflow/logs; "
            "if ps -ef | grep -v grep | grep -q 'streaming_to_hdfs.py'; then "
            "echo 'Streaming ingest already running'; "
            "else "
            "nohup /opt/spark/bin/spark-submit --master $SPARK_MASTER_URL "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
            "--conf spark.jars.ivy=/tmp/ivy "
            "/opt/airflow/project/spark/streaming_to_hdfs.py "
            "--bootstrap-servers $KAFKA_BOOTSTRAP_SERVERS "
            "--topic $KAFKA_TOPIC "
            "--raw-path /data/raw/traffic "
            "--checkpoint-path /data/checkpoints/traffic_raw "
            "--bad-records-path /data/bad/traffic_raw "
            "--starting-offsets latest "
            "--trigger-interval '30 seconds' "
            "> /opt/airflow/logs/traffic_streaming.log 2>&1 & "
            "echo $! > /opt/airflow/logs/traffic_streaming.pid; "
            "fi\""
        ),
        env={
            "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
            "KAFKA_BOOTSTRAP_SERVERS": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "KAFKA_TOPIC": os.getenv("KAFKA_TOPIC", "traffic-events"),
        },
    )

    wait_for_data = BashOperator(
        task_id="wait_for_data",
        bash_command="sleep 60",
    )

    process_kpis = BashOperator(
        task_id="spark_process_kpis",
        bash_command=(
            "/opt/spark/bin/spark-submit --master $SPARK_MASTER_URL "
            "/opt/airflow/project/spark/process_traffic.py "
            "--raw-path /data/raw/traffic "
            "--processed-path /data/processed/traffic "
            "--analytics-path /data/analytics/traffic "
            "--bad-records-path /data/bad/traffic_processed"
        ),
        env={
            "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
        },
    )

    load_to_postgres = BashOperator(
        task_id="load_kpis_to_postgres",
        bash_command=(
            "/opt/spark/bin/spark-submit --master $SPARK_MASTER_URL "
            "--packages org.postgresql:postgresql:42.7.3 "
            "--conf spark.jars.ivy=/tmp/ivy "
            "/opt/airflow/project/spark/load_kpis_to_postgres.py "
            "--analytics-path /data/analytics/traffic "
            "--jdbc-url jdbc:postgresql://postgres:5432/$TRAFFIC_DB "
            "--jdbc-user $TRAFFIC_DB_USER "
            "--jdbc-password $TRAFFIC_DB_PASSWORD"
        ),
        env={
            "SPARK_MASTER_URL": os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"),
            "TRAFFIC_DB": os.getenv("TRAFFIC_DB", "traffic"),
            "TRAFFIC_DB_USER": os.getenv("TRAFFIC_DB_USER", "traffic_user"),
            "TRAFFIC_DB_PASSWORD": os.getenv("TRAFFIC_DB_PASSWORD", "traffic_pass"),
        },
    )

    validate = PythonOperator(task_id="validate_kpi_tables", python_callable=validate_kpis)

    wait_stack >> create_topic >> start_generator >> start_streaming >> wait_for_data >> process_kpis >> load_to_postgres >> validate
