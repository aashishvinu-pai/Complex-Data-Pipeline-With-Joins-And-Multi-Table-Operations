import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import ai.prevalent.sdspecore.sparkbase.table.iceberg.SDSIcebergReader

object DataQualityJob {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("NYC Taxi - Data Quality Checks")
      .master("local[*]")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
      .config("spark.sql.catalog.iceberg_catalog.warehouse", "/home/aashishvinu/tasks/multitable_iceberg/spark-warehouse")
      .config("spark.sql.defaultCatalog", "iceberg_catalog")
      .getOrCreate()

    import spark.implicits._

    val reader = new SDSIcebergReader(spark)

    try {

      val fact = reader.read("default.nyc_taxi_fact")
      val totalRows = fact.count()

      val missingPickupBorough = fact.filter($"pickup_borough".isNull).count()
      val missingDropoffBorough = fact.filter($"dropoff_borough".isNull).count()
      val invalidDuration = fact.filter(
        $"trip_duration_minutes" <= 0 || $"trip_duration_minutes" >= 1440
      ).count()

      if (missingPickupBorough + missingDropoffBorough + invalidDuration == 0) {
        logger.info("All checks passed — data looks clean!")
      } else {
        logger.warn("Some quality issues detected — review logs")
      }

    } catch {
      case e: Throwable =>
        logger.error("Data quality check failed", e)
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }
}