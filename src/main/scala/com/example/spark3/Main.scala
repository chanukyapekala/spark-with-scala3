package com.example.spark3

import com.example.spark3.models.Person
import com.example.spark3.services.DataProcessor
import com.example.spark3.utils.SparkSessionBuilder
import org.apache.logging.log4j.LogManager

/**
 * Main application demonstrating Scala 3 with Apache Spark
 */
object Main {

  private val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSessionBuilder.createLocalSession("Scala 3 Spark Example")

    try {
      logger.info("Starting Scala 3 Spark application")

      // Initialize data processor
      val processor = DataProcessor(spark)

      // Load data
      logger.info("Loading data from CSV file")
      val peopleDF = processor.loadFromCSV("data/sample.csv")

      logger.info("Original Data:")
      peopleDF.show()

      // Filter adults (age >= 18)
      logger.info("Filtering adults (age >= 18)")
      val adults = processor.filterByAge(peopleDF, minAge = 18)

      logger.info("Adults (age >= 18):")
      adults.show()

      // Calculate average age by city
      logger.info("Calculating average age by city")
      val avgAgeByCity = processor.averageAgeByCity(peopleDF)

      logger.info("Average Age by City:")
      avgAgeByCity.show()

      // Type-safe processing: convert to case classes
      logger.info("Type-safe processing: converting to case classes")
      val people: Seq[Person] = processor.toTypedCollection(peopleDF)

      logger.info("Person Summaries (Type-Safe):")
      people.foreach { p =>
        logger.info(s"${p.name} is ${p.age} years old and lives in ${p.city}")
      }

      // Formatted output using DataFrame operations
      logger.info("Generating formatted output")
      val summary = processor.createSummary(peopleDF)
      summary.show(truncate = false)

      logger.info("Application completed successfully")

    } finally {
      logger.info("Stopping Spark session")
      spark.stop()
    }
  }
}