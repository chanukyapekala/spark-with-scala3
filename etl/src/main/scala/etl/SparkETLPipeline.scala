package etl

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import shared.config.Paths
import shared.schemas.PersonSchema
import org.apache.logging.log4j.LogManager

/**
 * ETL Pipeline using Spark (Scala 2.13)
 *
 * This module reads the Parquet files created by the preprocessing module
 * and performs Spark operations (aggregations, joins, etc.)
 *
 * Key points:
 * - Uses Scala 2.13 (full Spark compatibility)
 * - All aggregations work perfectly
 * - No varargs issues
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
      .getOrCreate()

    try {
      // Read processed Parquet from preprocessing module
      val inputPath = Paths.People.processedParquet
      logger.info(s"Reading Parquet from: $inputPath")

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

  /**
   * Example of complex transformation
   */
  def analyzeByStatus(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._

    df.groupBy($"status", $"city")
      .agg(
        count("*").as("count"),
        avg($"age").as("avg_age")
      )
      .orderBy($"status", desc("count"))
  }

  /**
   * Example of window functions
   */
  def rankPeopleByCity(df: DataFrame): DataFrame = {
    import df.sparkSession.implicits._
    import org.apache.spark.sql.expressions.Window

    val windowSpec = Window.partitionBy($"city").orderBy($"age".desc)

    df.withColumn("age_rank_in_city", rank().over(windowSpec))
      .withColumn("age_dense_rank_in_city", dense_rank().over(windowSpec))
  }
}

/**
 * Additional Spark job examples
 */
object SparkJobs {
  private val logger = LogManager.getLogger(getClass)

  /**
   * Simple aggregation job
   */
  def simpleStats(spark: SparkSession, inputPath: String): Unit = {
    import spark.implicits._

    val df = spark.read.parquet(inputPath)

    logger.info("Computing simple statistics...")

    val stats = df.agg(
      count("*").as("total"),
      avg($"age").as("avg_age"),
      min($"age").as("min_age"),
      max($"age").as("max_age")
    )

    stats.show(truncate = false)
  }

  /**
   * Filter and transform job
   */
  def filterAdults(spark: SparkSession, inputPath: String, outputPath: String): Unit = {
    import spark.implicits._

    val df = spark.read.parquet(inputPath)

    val adults = df
      .filter($"age" >= 18)
      .filter($"status" === "Active")
      .select("id", "name", "email", "age", "city")

    logger.info(s"Filtered ${adults.count()} adults")

    adults.write.mode("overwrite").parquet(outputPath)
  }
}