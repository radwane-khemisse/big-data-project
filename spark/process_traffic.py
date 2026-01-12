import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    expr,
    floor,
    from_unixtime,
    sum as sum_,
    to_date,
    to_timestamp,
    unix_timestamp,
    when,
)


def main():
    hdfs_base = os.getenv("HDFS_BASE", "hdfs://namenode:8020")
    parser = argparse.ArgumentParser(description="Spark batch processing for traffic KPIs")
    parser.add_argument("--raw-path", default=f"{hdfs_base}/data/raw/traffic")
    parser.add_argument("--processed-path", default=f"{hdfs_base}/data/processed/traffic")
    parser.add_argument("--analytics-path", default=f"{hdfs_base}/data/analytics/traffic")
    parser.add_argument("--bad-records-path", default=f"{hdfs_base}/data/bad/traffic_processed")
    parser.add_argument("--wait-seconds", type=int, default=120)
    parser.add_argument("--wait-interval", type=int, default=5)
    parser.add_argument("--lookback-minutes", type=int, default=60)
    parser.add_argument("--occupancy-threshold", type=float, default=70.0)
    parser.add_argument("--speed-threshold", type=float, default=20.0)
    parser.add_argument("--congestion-threshold", type=float, default=0.6)
    args = parser.parse_args()

    def normalize_path(path):
        if "://" in path:
            return path
        if path.startswith("/"):
            return f"{hdfs_base}{path}"
        return path

    args.raw_path = normalize_path(args.raw_path)
    args.processed_path = normalize_path(args.processed_path)
    args.analytics_path = normalize_path(args.analytics_path)
    if args.bad_records_path:
        args.bad_records_path = normalize_path(args.bad_records_path)

    spark = SparkSession.builder.appName("traffic-processing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    if args.wait_seconds > 0:
        conf = spark._jsc.hadoopConfiguration()
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(args.raw_path)
        fs = hadoop_path.getFileSystem(conf)
        start = time.time()
        while True:
            if fs.exists(hadoop_path):
                entries = list(fs.listStatus(hadoop_path))
                if entries:
                    break
            if time.time() - start >= args.wait_seconds:
                raise RuntimeError(
                    f"Raw path not ready after {args.wait_seconds}s: {args.raw_path}"
                )
            time.sleep(args.wait_interval)

    df = spark.read.parquet(args.raw_path)

    df = df.withColumn("event_time", to_timestamp(col("event_time")))
    df = df.withColumn("dt", to_date(col("event_time")))

    valid = (
        col("event_time").isNotNull()
        & col("sensor_id").isNotNull()
        & col("road_id").isNotNull()
        & col("zone").isNotNull()
        & (col("vehicle_count") >= 0)
        & (col("average_speed") >= 0)
        & (col("occupancy_rate") >= 0)
        & (col("occupancy_rate") <= 100)
    )

    bad = df.filter(~valid)
    if args.bad_records_path:
        bad.write.mode("append").json(args.bad_records_path)

    clean = df.filter(valid)
    clean = clean.withColumn(
        "event_hour",
        from_unixtime(floor(unix_timestamp(col("event_time")) / 300) * 300).cast(
            "timestamp"
        ),
    )

    windowed = clean
    if args.lookback_minutes and args.lookback_minutes > 0:
        windowed = clean.filter(
            expr(
                f"event_time >= current_timestamp() - interval {args.lookback_minutes} minutes"
            )
        )

    windowed.write.mode("append").partitionBy("dt", "zone").parquet(
        args.processed_path
    )

    traffic_by_zone = (
        windowed.groupBy("dt", "event_hour", "zone")
        .agg(
            avg("vehicle_count").alias("avg_vehicle_count"),
            sum_("vehicle_count").alias("total_vehicle_count"),
            avg("average_speed").alias("avg_speed"),
            avg("occupancy_rate").alias("avg_occupancy"),
            count("*").alias("event_count"),
        )
    )

    speed_by_road = (
        windowed.groupBy("dt", "event_hour", "road_id", "road_type")
        .agg(
            avg("average_speed").alias("avg_speed"),
            avg("vehicle_count").alias("avg_vehicle_count"),
            count("*").alias("event_count"),
        )
    )

    congestion_flag = when(
        (col("occupancy_rate") >= args.occupancy_threshold)
        | (col("average_speed") <= args.speed_threshold),
        1,
    ).otherwise(0)

    congestion_by_zone = (
        windowed.withColumn("is_congested", congestion_flag)
        .groupBy("dt", "event_hour", "zone")
        .agg(
            avg("is_congested").alias("congestion_rate"),
            avg("occupancy_rate").alias("avg_occupancy"),
            avg("average_speed").alias("avg_speed"),
            count("*").alias("event_count"),
        )
    )

    critical_zones = congestion_by_zone.filter(
        col("congestion_rate") >= args.congestion_threshold
    )

    traffic_by_zone.write.mode("overwrite").partitionBy("dt", "zone").parquet(
        f"{args.analytics_path}/traffic_by_zone"
    )
    speed_by_road.write.mode("overwrite").partitionBy("dt", "road_type").parquet(
        f"{args.analytics_path}/speed_by_road"
    )
    congestion_by_zone.write.mode("overwrite").partitionBy("dt", "zone").parquet(
        f"{args.analytics_path}/congestion_by_zone"
    )
    critical_zones.write.mode("overwrite").partitionBy("dt", "zone").parquet(
        f"{args.analytics_path}/critical_zones"
    )

    spark.stop()


if __name__ == "__main__":
    main()
