import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import ai.prevalent.sdspecore.sparkbase.table.iceberg.{SDSIcebergReader, SDSIcebergWriter}

object AggregationJob {

  private val logger = LoggerFactory.getLogger(getClass)

  val FACT_TABLE           = "default.nyc_taxi_fact"
  val BOROUGH_ANALYSIS     = "default.nyc_taxi_borough_analysis"
  val TIME_ANALYSIS        = "default.nyc_taxi_time_analysis"
  val BOROUGH_PAIRS        = "default.nyc_taxi_borough_pairs"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Star Schema Aggregations")
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

    try {

      if (!spark.catalog.tableExists(FACT_TABLE)) {
        logger.error(s"Fact table $FACT_TABLE not found. Run FactJob first.")
        sys.exit(1)
      }

      logger.info(s"Loading fact table: $FACT_TABLE")
      val fact = reader.read(FACT_TABLE)
      logger.info(s"Loaded ${fact.count()} rows from fact table")

      val boroughAgg = fact.groupBy(
        $"pickup_date_key", $"pickup_borough", $"dropoff_borough"
      ).agg(
        count("*").as("total_trips"),
        sum("total_amount").as("total_revenue"),
        avg("trip_distance").as("avg_trip_distance"),
        avg("trip_duration_minutes").as("avg_trip_duration"),
        sum("passenger_count").cast("long").as("total_passengers")
      )

      writer.append(
        df = boroughAgg,
        tableName = BOROUGH_ANALYSIS,
        partitionCols = Array($"pickup_date_key")
      )
      logger.info(s"Borough analysis written — ${boroughAgg.count()} rows")

      val timeAgg = fact.groupBy(
        $"year", $"quarter", $"month", $"day_of_week", $"is_weekend"
      ).agg(
        count("*").as("trip_count"),
        sum("total_amount").as("total_revenue"),
        avg("total_amount").as("avg_fare")
      )

      writer.append(
        df = timeAgg,
        tableName = TIME_ANALYSIS,
        partitionCols = Array($"year", $"quarter")
      )
      logger.info(s"Time analysis written — ${timeAgg.count()} rows")


      val pairAgg = fact.groupBy(
        $"pickup_borough", $"dropoff_borough"
      ).agg(
        count("*").as("trip_count"),
        avg("trip_distance").as("avg_distance"),
        avg("total_amount").as("avg_fare"),
        avg("trip_duration_minutes").as("avg_duration")
      )
        .orderBy(desc("trip_count"))
        .limit(50)

      writer.overwritePartition(
        df = pairAgg,
        tableName = BOROUGH_PAIRS
      )
      logger.info(s"Top borough pairs overwritten — ${pairAgg.count()} rows")

      logger.info("Aggregation job completed successfully")

    } catch {
      case e: Throwable =>
        logger.error("Aggregation job failed", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}