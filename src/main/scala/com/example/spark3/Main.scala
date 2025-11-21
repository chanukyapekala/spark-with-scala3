package com.example.spark3

import com.example.spark3.models.Person
import com.example.spark3.services.DataProcessor
import com.example.spark3.utils.SparkSessionBuilder

/**
 * Main application demonstrating Scala 3 with Apache Spark
 */
object Main {

  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSessionBuilder.createLocalSession("Scala 3 Spark Example")

    try {
      // Initialize data processor
      val processor = DataProcessor(spark)

      // Load data
      val peopleDF = processor.loadFromCSV("data/sample.csv")

      println("\n=== Original Data ===")
      peopleDF.show()

      // Filter adults (age >= 18)
      val adults = processor.filterByAge(peopleDF, minAge = 18)

      println("\n=== Adults (age >= 18) ===")
      adults.show()

      // Calculate average age by city
      val avgAgeByCity = processor.averageAgeByCity(peopleDF)

      println("\n=== Average Age by City ===")
      avgAgeByCity.show()

      // Type-safe processing: convert to case classes
      val people: Seq[Person] = processor.toTypedCollection(peopleDF)

      println("\n=== Person Summaries (Type-Safe) ===")
      people.foreach { p =>
        println(s"${p.name} is ${p.age} years old and lives in ${p.city}")
      }

      // Formatted output using DataFrame operations
      println("\n=== Formatted Output ===")
      val summary = processor.createSummary(peopleDF)
      summary.show(truncate = false)

    } finally {
      spark.stop()
    }
  }
}