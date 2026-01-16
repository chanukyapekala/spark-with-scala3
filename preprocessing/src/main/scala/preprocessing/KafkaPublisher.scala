package preprocessing

import preprocessing.models.{Person, PersonStatus, types}
import shared.kafka.{PersonEvent, KafkaConfig}
import cats.effect.{IO, IOApp}
import org.apache.logging.log4j.LogManager
import java.time.Instant
import java.util.UUID
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties

/**
 * Kafka Publisher for PersonEvent messages
 * Demonstrates publishing test data to Kafka with Scala 3
 */
object KafkaPublisher:
  private val logger = LogManager.getLogger(getClass)

  /**
   * Transform Person (internal model) → PersonEvent (Kafka schema)
   * Adds event timestamp for event-time processing in Flink
   */
  def personToEvent(person: Person): PersonEvent =
    import types.* // Import extension methods for Age and other opaque types
    PersonEvent(
      id = person.id.toString,
      name = person.name,
      email = person.email.value,
      age = person.age.value, // Extract Int from opaque type Age
      city = person.city.value,
      status = person.status.toStorageString,
      createdAt = person.createdAt.toString,
      eventTime = System.currentTimeMillis()
    )

  /**
   * Generate test Person records for publishing
   */
  def generateTestPeople(count: Int): List[Person] =
    (1 to count).toList.flatMap { i =>
      val names = Array("Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry")
      val cities = Array("San Francisco", "New York", "Seattle", "Austin", "Boston", "Chicago")
      val domains = Array("example.com", "test.com", "demo.org")

      val name = names(i % names.length) + s" Smith-$i"
      val email = s"user$i@${domains(i % domains.length)}"
      val age = 18 + (i % 50)
      val city = cities(i % cities.length)

      Person.create(name, email, age, city).fold(_ => None, Some(_)).toList
    }

  /**
   * Publish PersonEvent messages to Kafka
   * Returns (successCount, failureCount)
   */
  def publishEvents(people: List[Person]): IO[(Int, Int)] = IO.blocking {
    val props = new Properties()
    props.put("bootstrap.servers", KafkaConfig.bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "1")
    props.put("retries", "3")

    var success = 0
    var failures = 0

    val producer = new KafkaProducer[String, String](props)
    try {
      for {
        person <- people
      } {
        val event = personToEvent(person)
        val record = new ProducerRecord[String, String](
          KafkaConfig.PEOPLE_TOPIC,
          person.id.toString,
          PersonEvent.toJson(event)
        )

        try {
          producer.send(record).get()
          success += 1
        } catch {
          case e: Exception =>
            logger.error(s"Failed to send record for person ${person.id}: ${e.getMessage}")
            failures += 1
        }
      }
    } finally {
      producer.flush()
      producer.close()
    }

    (success, failures)
  }

  /**
   * Publish events from a local file (JSON Lines format)
   */
  def publishFromFile(filePath: String): IO[(Int, Int)] =
    for {
      peopleOpt <- IO {
        try {
          val content = scala.io.Source.fromFile(filePath).mkString
          val lines = content.split("\n").filter(_.nonEmpty)
          import io.circe.parser._
          val people = lines.flatMap { line =>
            parse(line).toOption.flatMap(_.as[Person].toOption)
          }
          Some(people.toList)
        } catch {
          case e: Exception =>
            logger.error(s"Failed to read file $filePath: ${e.getMessage}")
            None
        }
      }
      result <- peopleOpt match
        case Some(people) =>
          logger.info(s"Publishing ${people.length} events from file")
          publishEvents(people)
        case None =>
          IO.pure((0, 0))
    } yield result

/**
 * Standalone app to publish test data to Kafka
 */
object PublishToKafka extends IOApp.Simple:
  private val logger = LogManager.getLogger(getClass)

  def run: IO[Unit] =
    logger.info("=" * 60)
    logger.info("Publishing Test Data to Kafka")
    logger.info("=" * 60)

    val people = KafkaPublisher.generateTestPeople(100)
    logger.info(s"Generated ${people.length} test Person records")

    for {
      (success, failures) <- KafkaPublisher.publishEvents(people)
      _ <- IO {
        logger.info("=" * 60)
        logger.info(s"Published to Kafka: $success successful, $failures failed")
        logger.info("=" * 60)
        if failures > 0 then
          logger.warn(s"Some messages failed to publish: $failures")
      }
    } yield ()