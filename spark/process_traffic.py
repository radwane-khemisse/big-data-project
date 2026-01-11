import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    floor,
    from_unixtime,
    sum as sum_,
    to_date,
    to_timestamp,
    unix_timestamp,
    when,
)


def main():
    parser = argparse.ArgumentParser(description="Spark batch processing for traffic KPIs")
    parser.add_argument("--raw-path", default="/data/raw/traffic")
    parser.add_argument("--processed-path", default="/data/processed/traffic")
    parser.add_argument("--analytics-path", default="/data/analytics/traffic")
    parser.add_argument("--bad-records-path", default="/data/bad/traffic_processed")
    parser.add_argument("--occupancy-threshold", type=float, default=70.0)
    parser.add_argument("--speed-threshold", type=float, default=20.0)
    parser.add_argument("--congestion-threshold", type=float, default=0.6)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("traffic-processing").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

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

    clean.write.mode("append").partitionBy("dt", "zone").parquet(args.processed_path)

    traffic_by_zone = (
        clean.groupBy("dt", "event_hour", "zone")
        .agg(
            avg("vehicle_count").alias("avg_vehicle_count"),
            sum_("vehicle_count").alias("total_vehicle_count"),
            avg("average_speed").alias("avg_speed"),
            avg("occupancy_rate").alias("avg_occupancy"),
            count("*").alias("event_count"),
        )
    )

    speed_by_road = (
        clean.groupBy("dt", "event_hour", "road_id", "road_type")
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
        clean.withColumn("is_congested", congestion_flag)
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
