# Assumptions and Clarifications

- Kafka uses Zookeeper-based setup for local simplicity.
- Event timestamps are generated in UTC with format YYYY-MM-DD HH:MM:SS.
- KPI aggregation window is hourly (event_hour) and partitioned by dt and zone.
- Congestion is defined as occupancy_rate >= 70 or average_speed <= 20 km/h.
- Critical zones are zones with congestion_rate >= 0.60.
- Raw, processed, and analytics data are written to HDFS paths specified in the PDF.
