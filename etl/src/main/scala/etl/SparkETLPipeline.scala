package etl

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import shared.config.Paths
import shared.schemas.PersonSchema
import org.apache.logging.log4j.LogManager

/**
 * ETL Pipeline using Spark (Scala 2.13)
 *
 * This module reads Parquet files from the data lake (written by Flink)
 * and performs batch analytics (aggregations, joins, etc.)
 *
 * Key points:
 * - Uses Scala 2.13 (full Spark compatibility + better forward-compatibility)
 * - Depends on shared module (Scala 2.13) for schemas and paths
 * - All aggregations work perfectly
 * - Can deploy to Databricks/EMR
 */
object SparkETLPipeline {
  private val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("=" * 60)
    logger.info("Spark ETL Pipeline Starting (Scala 2.13)")
    logger.info("=" * 60)

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Spark-with-Scala3-ETL")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.parquet.mergeSchema", "true") // Handle schema evolution
      .getOrCreate()

    try {
      // Read Parquet from data lake (written by Flink)
      val inputPath = Paths.People.streamingParquet
      logger.info(s"Reading Parquet from: $inputPath")

      // Spark will infer the schema from Parquet files
      val df = spark.read.parquet(inputPath)

      logger.info(s"Loaded ${df.count()} records")
      df.printSchema()

      // Perform ETL operations
      val stats = computeStatistics(df)
      val enriched = enrichData(df)

      // Write results
      val outputPath = Paths.People.outputStats
      logger.info(s"Writing statistics to: $outputPath")

      stats.write
        .mode("overwrite")
        .parquet(outputPath)

      // Show results
      logger.info("\nCity Statistics:")
      stats.show(truncate = false)

      logger.info("\nEnriched Data Sample:")
      enriched.select("name", "age", "city", "age_group", "email_domain")
        .show(10, truncate = false)

      logger.info("=" * 60)
      logger.info("ETL Pipeline Complete!")
      logger.info("=" * 60)

    } finally {
      spark.stop()
    }
  }

  /**
   * Compute statistics by city
   * This demonstrates aggregations that work perfectly in Scala 2.13
   */
  def computeStatistics(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    logger.info("Computing city statistics...")

    // Multiple aggregations - works perfectly in Scala 2.13!
    df.groupBy($"city")
      .agg(
        count("*").as("total_people"),
        avg($"age").as("avg_age"),
        min($"age").as("min_age"),
        max($"age").as("max_age"),
        stddev($"age").as("stddev_age"),
        countDistinct($"status").as("distinct_statuses")
      )
      .orderBy(desc("total_people"))
  }

  /**
   * Enrich data with computed columns
   */
  def enrichData(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    logger.info("Enriching data...")

    df.withColumn("age_group",
        when($"age" < 18, "Minor")
        .when($"age" < 30, "Young Adult")
        .when($"age" < 50, "Adult")
        .when($"age" < 65, "Middle Age")
        .otherwise("Senior")
      )
      .withColumn("email_domain",
        split($"email", "@").getItem(1)
      )
      .withColumn("name_length", length($"name"))
      .withColumn("is_active", $"status" === "Active")
  }

}