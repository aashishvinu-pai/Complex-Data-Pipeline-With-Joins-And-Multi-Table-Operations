from pyspark.sql import functions as F
import findspark
findspark.init("/opt/spark")

from pyspark.sql import SparkSession

# ───────────────────────────────────────────────
#          Spark + Iceberg Session Setup
# ───────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("Iceberg Tables Quick Verification") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.ice_hadoop", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.ice_hadoop.type", "hadoop") \
    .config("spark.sql.catalog.ice_hadoop.warehouse", "file:/home/aashishvinu/tasks/spark_iceberg/spark-warehouse") \
    .config("spark.sql.defaultCatalog", "ice_hadoop") \
    .getOrCreate()


print("Spark session ready ✓")
print(f"Spark version: {spark.version}\n")

# ───────────────────────────────────────────────
#          Helper to display Iceberg table nicely
# ───────────────────────────────────────────────
def show_table_info(table_name, sample_rows=5, truncate=False):
    print(f"\n{'═' * 70}")
    print(f"TABLE: {table_name}")
    print(f"{'═' * 70}")
    
    try:
        if not spark.catalog.tableExists(table_name):
            print("→ Table does NOT exist in the catalog.")
            return
        
        df = spark.table(table_name)
        
        # Basic info
        print("Schema:")
        df.printSchema()
        
        count = df.count()
        print(f"\nTotal rows: {count:,}")
        
        if count == 0:
            print("→ Table is empty.")
            return
        
        # Sample data
        print(f"\nSample ({sample_rows} rows):")
        df.show(sample_rows, truncate=truncate)
        
        # Partition information (if any)
        print("\nPartition info:")
        spark.sql(f"""
            DESCRIBE TABLE EXTENDED {table_name}
        """).filter(
            "col_name IN ('Partition Columns', 'Partition Spec')"
        ).show(truncate=False)
        
    except Exception as e:
        print(f"Error reading {table_name}: {str(e)}")

# ───────────────────────────────────────────────
#          Check all assignment tables
# ───────────────────────────────────────────────
tables = [
    "nyc_taxi_trips_raw",
    "nyc_taxi_daily_summary",
    "nyc_taxi_hourly_patterns",
    "nyc_taxi_top_locations"
]

for table in tables:
    show_table_info(table)

# Optional: quick list of all tables in the catalog
print(f"\n{'─' * 70}")
print("All tables in catalog:")
spark.sql("SHOW TABLES").show(truncate=False)

print("\nDone.")


# ───────────────────────────────────────────────
#          Execution Command (Vrithi aakit add to readme)
# ───────────────────────────────────────────────

sbt clean update compile assembly
spark-submit --class IngestionJob --driver-memory 20g --executor-memory 20g target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar 
spark-submit --class AggregationJob --driver-memory 20g --executor-memory 20g target/scala-2.13/nyc-taxi-iceberg-assembly-1.0.jar
