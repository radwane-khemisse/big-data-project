import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def write_table(df, url, table, user, password, driver, mode):
    (
        df.write.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", driver)
        .option("truncate", "true")
        .mode(mode)
        .save()
    )


def read_or_empty(spark, path, schema):
    try:
        return spark.read.parquet(path)
    except Exception as exc:
        if "Unable to infer schema" in str(exc):
            return spark.createDataFrame([], schema)
        raise


def main():
    hdfs_base = os.getenv("HDFS_BASE", "hdfs://namenode:8020")
    parser = argparse.ArgumentParser(description="Load KPI tables into Postgres")
    parser.add_argument("--analytics-path", default=f"{hdfs_base}/data/analytics/traffic")
    parser.add_argument("--jdbc-url", required=True)
    parser.add_argument("--jdbc-user", required=True)
    parser.add_argument("--jdbc-password", required=True)
    parser.add_argument("--jdbc-driver", default="org.postgresql.Driver")
    parser.add_argument("--mode", default="overwrite")
    args = parser.parse_args()

    def normalize_path(path):
        if "://" in path:
            return path
        if path.startswith("/"):
            return f"{hdfs_base}{path}"
        return path

    args.analytics_path = normalize_path(args.analytics_path)

    spark = SparkSession.builder.appName("traffic-load-kpis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    traffic_by_zone_schema = StructType(
        [
            StructField("dt", DateType(), True),
            StructField("event_hour", TimestampType(), True),
            StructField("zone", StringType(), True),
            StructField("avg_vehicle_count", DoubleType(), True),
            StructField("total_vehicle_count", DoubleType(), True),
            StructField("avg_speed", DoubleType(), True),
            StructField("avg_occupancy", DoubleType(), True),
            StructField("event_count", LongType(), True),
        ]
    )
    speed_by_road_schema = StructType(
        [
            StructField("dt", DateType(), True),
            StructField("event_hour", TimestampType(), True),
            StructField("road_id", StringType(), True),
            StructField("road_type", StringType(), True),
            StructField("avg_speed", DoubleType(), True),
            StructField("avg_vehicle_count", DoubleType(), True),
            StructField("event_count", LongType(), True),
        ]
    )
    congestion_by_zone_schema = StructType(
        [
            StructField("dt", DateType(), True),
            StructField("event_hour", TimestampType(), True),
            StructField("zone", StringType(), True),
            StructField("congestion_rate", DoubleType(), True),
            StructField("avg_occupancy", DoubleType(), True),
            StructField("avg_speed", DoubleType(), True),
            StructField("event_count", LongType(), True),
        ]
    )

    traffic_by_zone = read_or_empty(
        spark, f"{args.analytics_path}/traffic_by_zone", traffic_by_zone_schema
    )
    speed_by_road = read_or_empty(
        spark, f"{args.analytics_path}/speed_by_road", speed_by_road_schema
    )
    congestion_by_zone = read_or_empty(
        spark, f"{args.analytics_path}/congestion_by_zone", congestion_by_zone_schema
    )
    critical_zones = read_or_empty(
        spark, f"{args.analytics_path}/critical_zones", congestion_by_zone_schema
    )

    write_table(
        traffic_by_zone,
        args.jdbc_url,
        "kpi_traffic_by_zone",
        args.jdbc_user,
        args.jdbc_password,
        args.jdbc_driver,
        args.mode,
    )
    write_table(
        speed_by_road,
        args.jdbc_url,
        "kpi_speed_by_road",
        args.jdbc_user,
        args.jdbc_password,
        args.jdbc_driver,
        args.mode,
    )
    write_table(
        congestion_by_zone,
        args.jdbc_url,
        "kpi_congestion_by_zone",
        args.jdbc_user,
        args.jdbc_password,
        args.jdbc_driver,
        args.mode,
    )
    write_table(
        critical_zones,
        args.jdbc_url,
        "kpi_critical_zones",
        args.jdbc_user,
        args.jdbc_password,
        args.jdbc_driver,
        args.mode,
    )

    spark.stop()


if __name__ == "__main__":
    main()
