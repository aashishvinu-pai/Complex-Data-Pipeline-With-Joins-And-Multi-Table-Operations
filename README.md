# NYC Taxi Data Pipeline with Apache Iceberg & Spark

Assignment 4: Complex Data Pipeline with Joins and Multi-Table Operations  
Star Schema implementation using Iceberg tables on local Spark setup.

## Project Overview

This project builds a **star schema** data warehouse for **NYC Yellow Taxi Trip Records** using **Apache Iceberg** tables managed via Spark.  

Main goals:
- Ingest raw Parquet data into Iceberg
- Create dimension tables (synthetic location + generated date)
- Build a wide/denormalized fact table with joins
- Generate analytical summary tables
- Include basic data quality validation

Tables created:
- `default.nyc_taxi_trips_raw`          (ingested & enriched raw trips)
- `default.nyc_taxi_location_dim`       (synthetic location dimension)
- `default.nyc_taxi_date_dim`           (date dimension from data range)
- `default.nyc_taxi_fact`               (core fact table with keys + attributes)
- `default.nyc_taxi_borough_analysis`   (borough-level aggregates)
- `default.nyc_taxi_time_analysis`      (time-based aggregates)
- `default.nyc_taxi_borough_pairs`      (top 50 borough pairs)

All tables use **Iceberg** format with Hadoop catalog (`iceberg_catalog`).

## Tech Stack

- **Scala 2.13**
- **Apache Spark** (local mode)
- **Apache Iceberg** (via `iceberg-spark-runtime-3.5_2.13:1.9.0`)
- **SDSIcebergReader / SDSIcebergWriter** (custom wrappers)
- **sbt** for build

## Project Structure

```
nyc-taxi-iceberg/
├── src/
│   └── main/
│       └── scala/
│           ├── IngestionJob.scala
│           ├── DimensionJob.scala
│           ├── FactJob.scala
│           ├── AggregationJob.scala
│           └── DataQualityJob.scala
├── data/
│   ├── input/                     ← Put your yellow_tripdata_*.parquet here
│   └── taxi_zone_lookup.csv       ← Optional (not used in synthetic version)
├── spark-warehouse/               ← Iceberg warehouse (auto-created)
├── target/                        ← sbt build output
└── build.sbt                      ← sbt configuration
```

## Prerequisites

- Java 11 or 17
- sbt 1.9+ installed
- ~3 months of NYC Yellow Taxi Parquet files (2025 recommended) placed in:
  ```
  /home/aashishvinu/tasks/multitable_iceberg/data/input/
  ```
  Example filenames: `yellow_tripdata_2025-01.parquet`, etc.

## Build the Project

```bash
# Clean, update dependencies, compile, and create fat JAR
sbt clean update compile assembly
```

Output JAR will be something like:  
`target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar`

## Execution Order

Run jobs **in this exact order**:

```bash
# 1. Ingest raw Parquet → create nyc_taxi_trips_raw
spark-submit --class IngestionJob \
  --driver-memory 20g --executor-memory 20g \
  target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar

# 2. Create synthetic location dim + date dim
spark-submit --class DimensionJob \
  --driver-memory 20g --executor-memory 20g \
  target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar

# 3. Build fact table with joins
spark-submit --class FactJob \
  --driver-memory 20g --executor-memory 20g \
  target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar

# 4. Generate analytical summary tables
spark-submit --class AggregationJob \
  --driver-memory 20g --executor-memory 20g \
  target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar

# 5. Run data quality checks (optional)
spark-submit --class DataQualityJob \
  --driver-memory 20g --executor-memory 20g \
  target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar
```

## Interactive Exploration (spark-shell)

Start a spark-shell with Iceberg support:

```bash
spark-shell --master "local[*]" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.iceberg_catalog.type=hadoop" \
  --conf "spark.sql.catalog.iceberg_catalog.warehouse=/home/aashishvinu/tasks/multitable_iceberg/spark-warehouse" \
  --conf "spark.sql.defaultCatalog=iceberg_catalog" \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.0,ch.qos.logback:logback-classic:1.5.12
```

### Useful verification queries

```sql
-- List all tables
SHOW TABLES IN iceberg_catalog.default;

-- Table schema
DESCRIBE iceberg_catalog.default.nyc_taxi_trips_raw;

-- Basic statistics from raw table
SELECT
  count(*) AS total_trips,
  count(DISTINCT pickup_date) AS distinct_dates,
  min(pickup_date) AS first_date,
  max(pickup_date) AS last_date,
  round(avg(trip_distance), 2) AS avg_distance_miles,
  round(avg(trip_duration_minutes), 1) AS avg_duration_minutes,
  round(avg(average_speed_mph), 1) AS avg_speed_mph,
  round(avg(total_amount), 2) AS avg_total_amount_usd
FROM iceberg_catalog.default.nyc_taxi_trips_raw;

-- Row count before & after last snapshot (example snapshot ID)
SELECT
  'current' AS version,
  count(*) AS cnt
FROM iceberg_catalog.default.nyc_taxi_trips_raw

UNION ALL

SELECT
  'previous' AS version,
  count(*) AS cnt
FROM iceberg_catalog.default.nyc_taxi_trips_raw
VERSION AS OF 8903115058753808214;
```

## Star Schema Design Summary

- **Dimensions** (small, descriptive)
  - `nyc_taxi_location_dim`: synthetic (location_id, location_name, borough, zone)
  - `nyc_taxi_date_dim`: generated from data range (date_key, year, quarter, month, day_of_week, is_weekend)

- **Fact** (wide / denormalized)
  - `nyc_taxi_fact`: trip measures + dimension keys + attributes (borough, zone, year, quarter, etc.)

- **Materialized views / summaries**
  - Borough-level aggregates
  - Time-based aggregates
  - Top borough pair analysis
