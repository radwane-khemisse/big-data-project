# Partitioning and Parquet

## Raw and processed zones
- HDFS raw and processed data are partitioned by:
  - dt (YYYY-MM-DD)
  - zone

## Analytics zone
- Zone-level KPIs are partitioned by dt and zone
- Road-level KPIs are partitioned by dt and road_type

## Why Parquet
- Columnar format optimized for analytics
- Efficient compression and lower storage footprint
- Predicate pushdown and faster scans for dashboards
