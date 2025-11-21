package com.example.spark3.utils

import org.apache.spark.sql.SparkSession

/**
 * Utility object for building and configuring Spark sessions
 */
object SparkSessionBuilder {

  /**
   * Create a local Spark session with sensible defaults
   * @param appName Name of the Spark application
   * @return Configured SparkSession
   */
  def createLocalSession(appName: String = "Scala 3 Spark Application"): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
  }
}