# Architecture Diagram

```mermaid
flowchart LR
  subgraph Sources
    S[Simulated Sensors]\nPython generator
  end

  subgraph Ingestion
    K[Kafka topic\ntraffic-events]
  end

  subgraph DataLake
    R[HDFS Raw\n/data/raw/traffic]
    P[HDFS Processed\n/data/processed/traffic]
    A[HDFS Analytics\n/data/analytics/traffic]
  end

  subgraph Processing
    SS[Spark Structured Streaming]\nKafka -> Raw
    SB[Spark Batch]\nKPIs + analytics
  end

  subgraph Serving
    PG[Postgres KPI tables]
    G[Grafana Dashboards]
  end

  subgraph Orchestration
    AF[Airflow DAG]
  end

  S --> K --> SS --> R --> SB --> P --> A --> PG --> G
  AF -.orchestrates.-> K
  AF -.orchestrates.-> SS
  AF -.orchestrates.-> SB
  AF -.orchestrates.-> PG
```
