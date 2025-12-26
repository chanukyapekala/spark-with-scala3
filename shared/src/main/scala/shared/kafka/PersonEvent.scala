package shared.kafka

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Kafka message for Person events
 * This is the schema for events published to Kafka and consumed by Flink
 *
 * Uses Jackson for JSON serialization (compatible with both Kafka and Flink)
 */
case class PersonEvent(
  @JsonProperty("id") id: String,
  @JsonProperty("name") name: String,
  @JsonProperty("email") email: String,
  @JsonProperty("age") age: Int,
  @JsonProperty("city") city: String,
  @JsonProperty("status") status: String,
  @JsonProperty("createdAt") createdAt: String,
  @JsonProperty("eventTime") eventTime: Long // Unix timestamp in milliseconds
)

object PersonEvent {
  /**
   * Jackson ObjectMapper configured for Scala
   * Shared across serialization and deserialization
   */
  val objectMapper: ObjectMapper = new ObjectMapper()
    .registerModule(DefaultScalaModule)
    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  /**
   * Serialize PersonEvent to JSON string
   */
  def toJson(event: PersonEvent): String = {
    objectMapper.writeValueAsString(event)
  }

  /**
   * Deserialize JSON string to PersonEvent
   */
  def fromJson(json: String): PersonEvent = {
    objectMapper.readValue(json, classOf[PersonEvent])
  }

  /**
   * Serialize PersonEvent to JSON bytes (for Kafka)
   */
  def toBytes(event: PersonEvent): Array[Byte] = {
    objectMapper.writeValueAsBytes(event)
  }

  /**
   * Deserialize JSON bytes to PersonEvent (from Kafka)
   */
  def fromBytes(bytes: Array[Byte]): PersonEvent = {
    objectMapper.readValue(bytes, classOf[PersonEvent])
  }
}
