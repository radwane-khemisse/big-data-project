import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def build_schema():
    return StructType(
        [
            StructField("sensor_id", StringType(), True),
            StructField("road_id", StringType(), True),
            StructField("road_type", StringType(), True),
            StructField("zone", StringType(), True),
            StructField("vehicle_count", IntegerType(), True),
            StructField("average_speed", DoubleType(), True),
            StructField("occupancy_rate", DoubleType(), True),
            StructField("event_time", StringType(), True),
        ]
    )


def main():
    hdfs_base = os.getenv("HDFS_BASE", "hdfs://namenode:8020")
    parser = argparse.ArgumentParser(description="Spark streaming: Kafka -> HDFS raw zone")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "traffic-events"))
    parser.add_argument("--raw-path", default=f"{hdfs_base}/data/raw/traffic")
    parser.add_argument(
        "--checkpoint-path", default=f"{hdfs_base}/data/checkpoints/traffic_raw"
    )
    parser.add_argument("--bad-records-path", default=f"{hdfs_base}/data/bad/traffic_raw")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--trigger-interval", default="30 seconds")
    parser.add_argument("--run-seconds", type=int, default=0)
    args = parser.parse_args()

    def normalize_path(path):
        if "://" in path:
            return path
        if path.startswith("/"):
            return f"{hdfs_base}{path}"
        return path

    args.raw_path = normalize_path(args.raw_path)
    args.checkpoint_path = normalize_path(args.checkpoint_path)
    if args.bad_records_path:
        args.bad_records_path = normalize_path(args.bad_records_path)

    spark = SparkSession.builder.appName("traffic-kafka-to-hdfs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = build_schema()

    raw_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    parsed = (
        raw_kafka.selectExpr("CAST(value AS STRING) as json_value")
        .withColumn("data", from_json(col("json_value"), schema))
    )

    good = parsed.filter(col("data").isNotNull()).select("data.*")
    bad = parsed.filter(col("data").isNull()).select(col("json_value").alias("raw_value"))

    good = good.withColumn("event_time", to_timestamp(col("event_time")))
    good = good.filter(col("event_time").isNotNull())
    good = good.withColumn("dt", to_date(col("event_time")))

    good_query = (
        good.writeStream.outputMode("append")
        .format("parquet")
        .option("path", args.raw_path)
        .option("checkpointLocation", args.checkpoint_path)
        .partitionBy("dt", "zone")
        .trigger(processingTime=args.trigger_interval)
        .start()
    )

    bad_query = (
        bad.writeStream.outputMode("append")
        .format("json")
        .option("path", args.bad_records_path)
        .option("checkpointLocation", f"{args.bad_records_path}/_checkpoints")
        .trigger(processingTime=args.trigger_interval)
        .start()
    )

    if args.run_seconds > 0:
        good_query.awaitTermination(args.run_seconds)
        good_query.stop()
        bad_query.stop()
        time.sleep(2)
    else:
        spark.streams.awaitAnyTermination()

    spark.stop()


if __name__ == "__main__":
    main()
