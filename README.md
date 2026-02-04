
sbt clean update compile assembly
spark-submit --class IngestionJob --driver-memory 20g --executor-memory 20g target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar
spark-submit --class DimensionJob --driver-memory 20g --executor-memory 20g target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar
spark-submit --class FactJob --driver-memory 20g --executor-memory 20g target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar
spark-submit --class AggregationJob --driver-memory 20g --executor-memory 20g target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar
spark-submit --class DataQualityJob --driver-memory 20g --executor-memory 20g target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar


spark-shell --master "local[*]" \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
--conf "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" \
--conf "spark.sql.catalog.iceberg_catalog.type=hadoop" \
--conf "spark.sql.catalog.iceberg_catalog.warehouse=/home/aashishvinu/tasks/multitable_iceberg/spark-warehouse" \
--conf "spark.sql.defaultCatalog=iceberg_catalog" \
--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.9.0,ch.qos.logback:logback-classic:1.5.12

# ───────────────────────────────────────────────
#          Execution Command (Vrithi aakit add to readme)
# ───────────────────────────────────────────────


spark.sql("DESCRIBE iceberg_catalog.default.nyc_taxi_trips_raw").show(50, false)

spark.sql("""
SELECT
count(*) AS total_trips,
count(DISTINCT pickup_date) AS distinct_dates,
min(pickup_date) AS first_date,
max(pickup_date) AS last_date,
round(avg(trip_distance), 2) AS avg_distance_miles,
round(avg(trip_duration_minutes), 1) AS avg_duration_minutes,
round(avg(average_speed_mph), 1) AS avg_speed_mph,
round(avg(total_amount), 2) AS avg_total_amount_usd
FROM iceberg_catalog.default.nyc_taxi_trips_raw
""").show(false)

spark.sql("""
SELECT
'current' AS version,
count(*) AS cnt
FROM iceberg_catalog.default.nyc_taxi_trips_raw

UNION ALL

SELECT
'previous' AS version,
count(*) AS cnt
FROM iceberg_catalog.default.nyc_taxi_trips_raw
VERSION AS OF 8903115058753808214
""").show()