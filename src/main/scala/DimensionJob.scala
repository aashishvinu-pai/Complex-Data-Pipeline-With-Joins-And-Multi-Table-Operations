import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import ai.prevalent.sdspecore.sparkbase.table.iceberg.{SDSIcebergReader, SDSIcebergWriter}

import java.time.LocalDate

object DimensionJob {

  private val logger = LoggerFactory.getLogger(getClass)

  val LOCATION_TABLE = "default.nyc_taxi_location_dim"
  val DATE_TABLE     = "default.nyc_taxi_date_dim"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Dimension Tables (Synthetic Location)")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
      .config("spark.sql.catalog.iceberg_catalog.warehouse", "/home/aashishvinu/tasks/multitable_iceberg/spark-warehouse")
      .config("spark.sql.defaultCatalog", "iceberg_catalog")
      .getOrCreate()

    import spark.implicits._

    val reader = new SDSIcebergReader(spark)
    val writer = new SDSIcebergWriter(spark)
    val raw = reader.read("default.nyc_taxi_trips_raw")

    try {
      logger.info("Starting dimension tables creation...")
//      spark.sql("CREATE SCHEMA IF NOT EXISTS iceberg_catalog.default")

      logger.info("Building synthetic nyc_taxi_location_dim from raw taxi data")

      val uniqueLocations = raw
        .select($"pickup_location_id".as("location_id"))
        .union(raw.select($"dropoff_location_id".as("location_id")))
        .distinct()
        .filter($"location_id".isNotNull && $"location_id" =!= lit(0))
        .withColumn("location_id", $"location_id".cast("int"))

      val locationDim = uniqueLocations
        .withColumn("location_name", concat(lit("Location "), $"location_id"))
        .withColumn("borough",
          when($"location_id".between(1, 50), "Manhattan")
            .when($"location_id".between(51, 100), "Brooklyn")
            .when($"location_id".between(101, 150), "Queens")
            .when($"location_id".between(151, 200), "Bronx")
            .otherwise("Staten Island")
        )
        .withColumn("zone", concat(lit("Zone "), $"location_id"))
        .select("location_id", "location_name", "borough", "zone")

      writer.overwritePartition(locationDim, LOCATION_TABLE)
      logger.info(s"Location dim created — ${locationDim.count()} rows")

      logger.info("Generating nyc_taxi_date_dim from actual data range")

      val dateBounds = raw.agg(
        min("pickup_date").as("min_date"),
        max("pickup_date").as("max_date")
      ).head()

      val minDate: LocalDate = dateBounds.getDate(0).toLocalDate
      val maxDate: LocalDate = dateBounds.getDate(1).toLocalDate

      val days = java.time.temporal.ChronoUnit.DAYS.between(minDate, maxDate) + 1

      logger.info(s"Date range found: $minDate to $maxDate ($days days)")

      // Fixed: cast to int instead of long
      val dateRange = spark.range(days)
        .select(
          date_add(lit(minDate.toString).cast("date"), $"id".cast("int")).as("full_date")
        )

      val dateDim = dateRange
        .withColumn("date_key",      $"full_date")
        .withColumn("year",          year($"full_date"))
        .withColumn("quarter",       quarter($"full_date"))
        .withColumn("month",         month($"full_date"))
        .withColumn("day_of_month",  dayofmonth($"full_date"))
        .withColumn("day_of_week",   dayofweek($"full_date"))
        .withColumn("is_weekend",    when(dayofweek($"full_date").isin(1, 7), true).otherwise(false))

      writer.overwritePartition(dateDim, DATE_TABLE)
      logger.info(s"Date dim created — ${dateDim.count()} rows ($minDate to $maxDate)")

      logger.info("Dimension job completed successfully")

    } catch {
      case e: Throwable =>
        logger.error("Dimension job failed", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}