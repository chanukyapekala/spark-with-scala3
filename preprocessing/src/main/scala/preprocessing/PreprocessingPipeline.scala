package preprocessing

import preprocessing.models.{Person, PersonStatus, types}
import preprocessing.processors.{Processor, DataProcessor, measure}
import shared.config.Paths
import cats.effect.{IO, IOApp}
import org.apache.logging.log4j.LogManager
import java.nio.file.{Paths as JPaths}
import java.time.Instant
import java.util.UUID
import io.circe.syntax.*

// ============================================
// FEATURE #11: IOApp (Cats Effect)
// ============================================

object PreprocessingPipeline extends IOApp.Simple:
  private val logger = LogManager.getLogger(getClass)

  def run: IO[Unit] =
    logger.info("=" * 60)
    logger.info("Preprocessing Pipeline Starting")
    logger.info("=" * 60)

    val inputPath = JPaths.get(Paths.People.rawJsonLines)
    val outputPath = JPaths.get(Paths.People.processedParquet)

    logger.info(s"Input: $inputPath")
    logger.info(s"Output: $outputPath")

    // Using the given instance automatically
    Processor.processJsonToParquetWithWrite(inputPath, outputPath)
      .flatMap { stats =>
        IO {
          logger.info("=" * 60)
          logger.info("Processing Complete!")
          logger.info(stats.toString)
          logger.info("=" * 60)

          if stats.successRate < 50 then
            logger.error("Warning: Success rate below 50%!")
            sys.exit(1)
        }
      }

// ============================================
// FEATURE #12: @main ANNOTATION
// ============================================

@main def generateTestData(count: Int = 1000): Unit =
  val logger = LogManager.getLogger("DataGenerator")

  logger.info(s"Generating $count test records...")

  val people = (1 to count).map { i =>
    val names = Array("Alice", "Bob", "Carol", "David", "Eve", "Frank", "Grace", "Henry")
    val cities = Array("San Francisco", "New York", "Seattle", "Austin", "Boston", "Chicago")
    val domains = Array("example.com", "test.com", "demo.org")

    val name = names(i % names.length) + s" Smith-$i"
    val email = s"user$i@${domains(i % domains.length)}"
    val age = 18 + (i % 50)
    val city = cities(i % cities.length)

    Person.create(name, email, age, city) match
      case Right(person) => person
      case Left(error) =>
        logger.error(s"Failed to create person: $error")
        throw new RuntimeException(error)
  }

  // Write as JSON Lines
  import java.nio.file.{Files, Paths as JPaths, StandardOpenOption}

  val outputPath = JPaths.get(Paths.People.rawJsonLines)
  Files.createDirectories(outputPath.getParent)

  val jsonLines = people.map(_.asJson.noSpaces).mkString("\n")
  Files.write(
    outputPath,
    jsonLines.getBytes,
    StandardOpenOption.CREATE,
    StandardOpenOption.TRUNCATE_EXISTING
  )

  logger.info(s"✓ Generated $count records to ${Paths.People.rawJsonLines}")
  logger.info("")
  logger.info("Next step: Run preprocessing pipeline")
  logger.info("  sbt \"preprocessing/run\"")

@main def publishTestDataToKafka(count: Int = 100): Unit =
  val logger = LogManager.getLogger("KafkaPublisher")

  logger.info("=" * 60)
  logger.info("Publishing Test Data to Kafka")
  logger.info("=" * 60)

  val people = KafkaPublisher.generateTestPeople(count)
  logger.info(s"Generated $count test Person records")

  // Use cats.effect to run the IO
  import cats.effect.unsafe.implicits.global
  val (success, failures) = KafkaPublisher.publishEvents(people).unsafeRunSync()

  logger.info("=" * 60)
  logger.info(s"Published to Kafka topic '${shared.kafka.KafkaConfig.PEOPLE_TOPIC}'")
  logger.info(s"  ✓ Success: $success")
  logger.info(s"  ✗ Failures: $failures")
  logger.info("=" * 60)

  if success > 0 then
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Start Flink to consume and process events")
    logger.info("  2. Check data lake: data/streaming/people/")
    logger.info("  3. Run ETL for batch analytics")

@main def demoScala3Features(): Unit =
  import preprocessing.models.*  // Import extension methods

  println("\n" + "=" * 60)
  println("SCALA 3 FEATURES DEMONSTRATION")
  println("=" * 60 + "\n")

  // FEATURE 1: Enums
  println("1. ENUMS WITH METHODS:")
  val active = PersonStatus.Active
  val suspended = PersonStatus.Suspended("Policy violation", Instant.now())
  println(s"   Active is available: ${active.isAvailable}")
  println(s"   Suspended is available: ${suspended.isAvailable}")

  // FEATURE 2: Opaque Types
  println("\n2. OPAQUE TYPES (Zero-cost type safety):")
  val validEmail = types.Email("user@example.com")
  val invalidEmail = types.Email("not-an-email")
  println(s"   Valid email: $validEmail")
  println(s"   Invalid email: $invalidEmail")

  validEmail.foreach { email =>
    println(s"   Domain: ${email.domain}")
    println(s"   Username: ${email.username}")
  }

  // FEATURE 3: Extension Methods
  println("\n3. EXTENSION METHODS:")
  val person = Person.create("John Doe", "john@test.com", 30, "Seattle")
  person.foreach { p =>
    println(s"   Original: ${p.toSummary}")
    println(s"   Anonymized: ${p.anonymize.toSummary}")
    println(s"   Is adult: ${p.isAdult}")
    println(s"   Age category: ${p.ageCategory}")
  }

  // FEATURE 4: For-comprehension with validation
  println("\n4. FOR-COMPREHENSION VALIDATION:")
  val result = Person.create("Jane Smith", "jane@example.com", 25, "Boston")
  result match
    case Right(p) => println(s"   ✓ Created: ${p.toSummary}")
    case Left(error) => println(s"   ✗ Error: $error")

  // FEATURE 5: Pattern matching
  println("\n5. PATTERN MATCHING:")
  person.foreach { p =>
    val category = categorize(p)
    println(s"   Category: $category")
  }

  // FEATURE 6: Union types
  println("\n6. UNION TYPES:")
  val age: Validated[types.Age] = types.Age(150)
  println(s"   Age 150: $age")

  println("\n" + "=" * 60)
  println("For more examples, see:")
  println("  preprocessing/src/main/scala/preprocessing/models/Person.scala")
  println("  preprocessing/src/main/scala/preprocessing/processors/DataProcessor.scala")
  println("=" * 60 + "\n")