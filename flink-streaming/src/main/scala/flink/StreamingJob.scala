package flink

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.functions.MapFunction

import shared.kafka.{KafkaConfig, PersonEvent}
import shared.config.Paths

import java.time.Duration
import org.apache.logging.log4j.LogManager
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecordBuilder

/**
 * Flink Streaming Job (Scala 3!)
 *
 * Architecture:
 * 1. Consumes PersonEvent messages from Kafka
 * 2. Deserializes JSON to PersonEvent objects
 * 3. Applies watermarking for event-time processing
 * 4. Converts to Avro GenericRecord
 * 5. Writes to Parquet files with date/hour partitioning
 * 6. Enables checkpointing for exactly-once semantics
 *
 * Key Scala 3 Features Used:
 * - New control structure syntax (if-then, try-catch)
 * - Extension methods (from shared module)
 * - Top-level definitions
 * - Scala 3 class syntax
 */
object StreamingJob:
  private val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit =
    logger.info("Starting Flink Streaming Job (Scala 3)")

    // Create the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Configure checkpointing for exactly-once semantics
    env.enableCheckpointing(KafkaConfig.Flink.CHECKPOINT_INTERVAL_MS)
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setMinPauseBetweenCheckpoints(KafkaConfig.Flink.MIN_PAUSE_BETWEEN_CHECKPOINTS)
    checkpointConfig.setCheckpointTimeout(KafkaConfig.Flink.CHECKPOINT_TIMEOUT_MS)

    logger.info(s"Checkpointing enabled with interval: ${KafkaConfig.Flink.CHECKPOINT_INTERVAL_MS}ms")

    // Parallelism configuration (can be overridden via -p flag)
    if env.getParallelism == -1 then
      env.setParallelism(2) // Default parallelism

    logger.info(s"Parallelism: ${env.getParallelism}")

    // Create Kafka source
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(KafkaConfig.bootstrapServers)
      .setTopics(KafkaConfig.PEOPLE_TOPIC)
      .setGroupId(KafkaConfig.FLINK_CONSUMER_GROUP)
      .setStartingOffsets(OffsetsInitializer.earliest())
      .setValueOnlyDeserializer(SimpleStringSchema())
      .build()

    logger.info(s"Kafka source configured: ${KafkaConfig.bootstrapServers}, topic: ${KafkaConfig.PEOPLE_TOPIC}")

    // Create watermark strategy for event time processing
    val watermarkStrategy = WatermarkStrategy
      .forBoundedOutOfOrderness[String](Duration.ofSeconds(10))
      .withTimestampAssigner((event: String, timestamp: Long) => {
        try
          val personEvent = PersonEvent.fromJson(event)
          personEvent.eventTime
        catch
          case e: Exception =>
            logger.error(s"Failed to extract timestamp from event: $event", e)
            System.currentTimeMillis()
      })

    // Read from Kafka and process events
    val stringStream: DataStream[String] = env
      .fromSource(kafkaSource, watermarkStrategy, "Kafka Source")

    val events: DataStream[PersonEvent] = stringStream
      .map(new PersonEventDeserializer())
      .name("Deserialize JSON to PersonEvent")

    logger.info("Event stream created")

    // Write events to Parquet files
    val outputPath = new Path(Paths.People.streamingParquet)
    logger.info(s"Output path: ${outputPath.getPath}")

    // Create Avro schema for PersonEvent
    val schema = new Schema.Parser().parse("""{
      "type": "record",
      "name": "PersonEvent",
      "namespace": "shared.kafka",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "city", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "createdAt", "type": "string"},
        {"name": "eventTime", "type": "long"}
      ]
    }""")

    // Convert PersonEvent to Avro GenericRecord
    val eventStream = events
      .map(new PersonEventToAvroMapper(schema))
      .name("Convert to Avro GenericRecord")

    // Write to Parquet files (rolls on checkpoint by default)
    val sink = StreamingFileSink
      .forBulkFormat(outputPath, ParquetAvroWriters.forGenericRecord(schema))
      .withBucketAssigner(AvroRecordBucketAssigner())
      .build()

    eventStream
      .addSink(sink)
      .name("Write to Parquet")

    logger.info("Parquet sink configured")

    // Execute the job
    logger.info("Executing Flink job: People Event Processing")
    env.execute("People Event Processing from Kafka to Data Lake")

end StreamingJob

/**
 * Deserializer for PersonEvent (Scala 3 MapFunction)
 */
class PersonEventDeserializer extends MapFunction[String, PersonEvent]:
  private val logger = LogManager.getLogger(getClass)

  override def map(json: String): PersonEvent =
    try
      PersonEvent.fromJson(json)
    catch
      case e: Exception =>
        logger.error(s"Failed to deserialize event: $json", e)
        throw e

/**
 * Convert PersonEvent to Avro GenericRecord (Scala 3 MapFunction)
 */
class PersonEventToAvroMapper(schema: Schema) extends MapFunction[PersonEvent, GenericRecord]:
  override def map(event: PersonEvent): GenericRecord =
    new GenericRecordBuilder(schema)
      .set("id", event.id)
      .set("name", event.name)
      .set("email", event.email)
      .set("age", event.age)
      .set("city", event.city)
      .set("status", event.status)
      .set("createdAt", event.createdAt)
      .set("eventTime", event.eventTime)
      .build()

/**
 * Bucket assigner for Avro GenericRecord with date/hour partitioning
 * Extracts eventTime from GenericRecord and creates: /dt=yyyy-MM-dd/hour=HH/
 * Uses simple string formatting to avoid serialization issues
 */
class AvroRecordBucketAssigner extends org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner[GenericRecord, String]:

  import java.time.Instant
  import java.time.LocalDateTime
  import java.time.ZoneId

  override def getBucketId(
    element: GenericRecord,
    context: org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context
  ): String =
    val eventTime = element.get("eventTime").asInstanceOf[Long]
    val instant = Instant.ofEpochMilli(eventTime)
    val localDateTime = LocalDateTime.ofInstant(instant, ZoneId.of("UTC"))
    val date = f"${localDateTime.getYear}%04d-${localDateTime.getMonthValue}%02d-${localDateTime.getDayOfMonth}%02d"
    val hour = f"${localDateTime.getHour}%02d"
    s"dt=$date/hour=$hour"

  override def getSerializer: org.apache.flink.core.io.SimpleVersionedSerializer[String] =
    org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer.INSTANCE

