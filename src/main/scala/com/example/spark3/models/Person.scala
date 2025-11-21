package com.example.spark3.models

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * Person case class representing a person entity
 * @param name Person's name
 * @param age Person's age
 * @param city City where the person lives
 */
case class Person(name: String, age: Int, city: String)

object Person {
  /**
   * Spark SQL schema definition for Person
   */
  val schema: StructType = StructType(Array(
    StructField("name", StringType, nullable = false),
    StructField("age", IntegerType, nullable = false),
    StructField("city", StringType, nullable = false)
  ))

  /**
   * Convert a Spark Row to Person instance
   * @param row Spark SQL Row
   * @return Person instance
   */
  def fromRow(row: Row): Person =
    Person(
      name = row.getString(0),
      age = row.getInt(1),
      city = row.getString(2)
    )
}