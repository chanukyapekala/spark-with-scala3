package com.example.spark3.services

import com.example.spark3.models.Person
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Service for processing person data with Spark
 */
class DataProcessor(spark: SparkSession) {

  import spark.implicits._

  /**
   * Load person data from CSV file
   * @param path Path to CSV file
   * @return DataFrame containing person data
   */
  def loadFromCSV(path: String): DataFrame = {
    spark.read
      .option("header", "true")
      .schema(Person.schema)
      .csv(path)
  }

  /**
   * Filter persons by minimum age
   * @param df Input DataFrame
   * @param minAge Minimum age threshold
   * @return Filtered DataFrame
   */
  def filterByAge(df: DataFrame, minAge: Int): DataFrame = {
    df.filter($"age" >= minAge)
  }

  /**
   * Calculate average age grouped by city
   * @param df Input DataFrame
   * @return DataFrame with city, avg_age, and count columns
   */
  def averageAgeByCity(df: DataFrame): DataFrame = {
    df.groupBy($"city")
      .agg(
        avg($"age").as("avg_age"),
        count($"name").as("count")
      )
      .orderBy($"avg_age".desc)
  }

  /**
   * Create formatted summary strings for each person
   * @param df Input DataFrame
   * @return DataFrame with summary column
   */
  def createSummary(df: DataFrame): DataFrame = {
    df.select(
      concat(
        $"name",
        lit(" is "),
        $"age",
        lit(" years old and lives in "),
        $"city"
      ).as("summary")
    )
  }

  /**
   * Convert DataFrame to typed collection of Person instances
   * @param df Input DataFrame
   * @return Sequence of Person instances
   */
  def toTypedCollection(df: DataFrame): Seq[Person] = {
    df.collect().map(Person.fromRow).toSeq
  }
}