import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import ai.prevalent.sdspecore.sparkbase.table.iceberg.{SDSIcebergReader, SDSIcebergWriter}

object FactJob {

  private val logger = LoggerFactory.getLogger(getClass)

  val RAW_TABLE  = "default.nyc_taxi_trips_raw"
  val FACT_TABLE = "default.nyc_taxi_fact"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Build Fact Table with Joins")
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

      if (!spark.catalog.tableExists(RAW_TABLE)) {
        logger.error(s"Raw table $RAW_TABLE not found. Run ingestion first.")
        sys.exit(1)
      }

      logger.info(s"Reading from raw table: $RAW_TABLE")
      val raw = reader.read(RAW_TABLE)

      val locDim  = reader.read("default.nyc_taxi_location_dim")
      val dateDim = reader.read("default.nyc_taxi_date_dim")

      logger.info("Performing joins and cleaning...")

      val fact = raw
        .withColumn("trip_duration_minutes",
          (unix_timestamp($"dropoff_datetime") - unix_timestamp($"pickup_datetime")) / 60.0)
        .filter(
          $"trip_duration_minutes" > 0 &&
          $"trip_duration_minutes" < 1440 &&
          $"trip_distance" > 0 &&
          $"total_amount" > 0
        )
        .join(locDim.as("pu"), $"pickup_location_id" === $"pu.location_id", "left_outer")
        .join(locDim.as("do"), $"dropoff_location_id" === $"do.location_id", "left_outer")
        .join(dateDim, $"pickup_date" === $"date_key", "left_outer")
        .select(
          $"pickup_date".as("pickup_date_key"),
          $"pickup_location_id",
          $"dropoff_location_id",
          $"passenger_count",
          $"trip_distance",
          $"trip_duration_minutes",
          $"average_speed_mph",
          $"total_amount",
          $"fare_amount",
          $"tip_amount",
          $"pu.borough".as("pickup_borough"),
          $"pu.zone".as("pickup_zone"),
          $"do.borough".as("dropoff_borough"),
          $"do.zone".as("dropoff_zone"),
          $"year", $"quarter", $"month", $"day_of_week", $"is_weekend"
        )

      val rowCount = fact.count()
      logger.info(s"Prepared $rowCount fact records after cleaning and joins")

      writer.append(
        df = fact,
        tableName = FACT_TABLE,
        partitionCols = Array($"pickup_date_key")
      )

      logger.info("Fact table append completed successfully")

    } catch {
      case e: Throwable =>
        logger.error("Fact job failed", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}