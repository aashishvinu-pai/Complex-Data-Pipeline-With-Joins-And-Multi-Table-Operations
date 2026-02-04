import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import ai.prevalent.sdspecore.sparkbase.table.iceberg.{SDSIcebergReader, SDSIcebergWriter}

object DimensionJob {

  private val logger = LoggerFactory.getLogger(getClass)

  val LOCATION_TABLE = "default.nyc_taxi_location_dim"
  val DATE_TABLE     = "default.nyc_taxi_date_dim"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Dimension Tables")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
      .config("spark.sql.catalog.iceberg_catalog.warehouse", "/home/aashishvinu/tasks/spark_iceberg/spark-warehouse")
      .config("spark.sql.defaultCatalog", "iceberg_catalog")
      .getOrCreate()

    import spark.implicits._

    val reader = new SDSIcebergReader(spark)
    val writer = new SDSIcebergWriter(spark)

    try {
      logger.info("Starting dimension tables creation...")

      // ── LOCATION DIMENSION ───────────────────────────────────────────────
      logger.info("Building nyc_taxi_location_dim from taxi_zone_lookup.csv")

      val zonePath = "/home/aashishvinu/tasks/spark_iceberg/data/taxi_zone_lookup.csv"

      val locationDF = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(zonePath)
        .select(
          $"LocationID".cast("int").as("location_id"),
          $"Borough".as("borough"),
          $"Zone".as("zone"),
          $"service_zone"
        )
        .distinct()

      writer.overwritePartition(
        df = locationDF,
        tableName = LOCATION_TABLE
      )
      logger.info(s"Location dimension written — ${locationDF.count()} rows")

      // ── DATE DIMENSION ───────────────────────────────────────────────────
      logger.info("Generating nyc_taxi_date_dim from actual data range")

      val raw = reader.read("default.nyc_taxi_trips_raw")
      val dateBounds = raw.agg(
        min("pickup_date").as("min_date"),
        max("pickup_date").as("max_date")
      ).head()

      val minDate = dateBounds.getDate(0).toLocalDate
      val maxDate = dateBounds.getDate(1).toLocalDate

      val days = java.time.temporal.ChronoUnit.DAYS.between(minDate, maxDate) + 1

      val dateRange = spark.range(days)
        .select(
          (lit(minDate.toString).cast("date") + $"id".cast("long")).as("full_date")
        )

      val dateDim = dateRange
        .withColumn("date_key",      $"full_date")
        .withColumn("year",          year($"full_date"))
        .withColumn("quarter",       quarter($"full_date"))
        .withColumn("month",         month($"full_date"))
        .withColumn("day_of_month",  dayofmonth($"full_date"))
        .withColumn("day_of_week",   dayofweek($"full_date"))
        .withColumn("is_weekend",    when(dayofweek($"full_date").isin(1, 7), true).otherwise(false))

      writer.overwritePartition(
        df = dateDim,
        tableName = DATE_TABLE
      )
      logger.info(s"Date dimension written — ${dateDim.count()} rows (from $minDate to $maxDate)")

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