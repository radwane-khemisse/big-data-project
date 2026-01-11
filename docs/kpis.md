# KPI Definitions

## Traffic average by zone
- Metric: average vehicle count per zone per hour
- Table: kpi_traffic_by_zone
- Columns: event_hour, zone, avg_vehicle_count, total_vehicle_count, avg_speed, avg_occupancy, event_count

## Average speed by road
- Metric: average speed per road per hour
- Table: kpi_speed_by_road
- Columns: event_hour, road_id, road_type, avg_speed, avg_vehicle_count, event_count

## Congestion rate by zone
- Definition: share of events where occupancy_rate >= 70 OR average_speed <= 20
- Aggregated per zone per hour
- Table: kpi_congestion_by_zone
- Columns: event_hour, zone, congestion_rate, avg_speed, avg_occupancy, event_count

## Critical zones
- Definition: zones with congestion_rate >= 0.60
- Table: kpi_critical_zones
