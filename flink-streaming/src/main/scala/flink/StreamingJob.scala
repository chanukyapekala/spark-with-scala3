package flink

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.core.fs.Path
import org.apache.flink.api.common.functions.MapFunction

import shared.kafka.{KafkaConfig, PersonEvent}
import shared.config.Paths

import java.time.Duration
import org.apache.logging.log4j.LogManager

/**
 * Flink Streaming Job (Scala 3!)
 *
 * Architecture:
 * 1. Consumes PersonEvent messages from Kafka
 * 2. Deserializes JSON to PersonEvent objects
 * 3. Windows events by event time (1-minute tumbling windows)
 * 4. Writes to JSON files (later can be converted to Parquet)
 * 5. Enables checkpointing for exactly-once semantics
 *
 * Key Scala 3 Features Used:
 * - New control structure syntax
 * - Extension methods (from shared module)
 * - Opaque types (from shared module)
 * - Top-level definitions
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

    // Write events to files (JSON format)
    val outputPath = new Path(Paths.People.streamingParquet)

    logger.info(s"Output path: ${outputPath.getPath}")

    // Write to file system with rolling policy
    val sink = StreamingFileSink
      .forRowFormat(outputPath, new org.apache.flink.api.common.serialization.SimpleStringEncoder[PersonEvent]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withRolloverInterval(Duration.ofMinutes(5).toMillis)
          .withInactivityInterval(Duration.ofMinutes(2).toMillis)
          .withMaxPartSize(128 * 1024 * 1024) // 128 MB
          .build()
      )
      .withBucketAssigner(PersonEventBucketAssigner())
      .build()

    events
      .map(new PersonEventSerializer())
      .addSink(sink)
      .name("Write to Files")

    logger.info("Sink configured")

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
 * Serializer for PersonEvent (Scala 3 MapFunction)
 */
class PersonEventSerializer extends MapFunction[PersonEvent, String]:
  override def map(event: PersonEvent): String =
    PersonEvent.toJson(event)

/**
 * Custom bucket assigner for partitioning by date/hour (Scala 3 style)
 * Creates directory structure: /dt=yyyy-MM-dd/hour=HH/
 */
class PersonEventBucketAssigner
  extends org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner[PersonEvent, String]:

  import java.time.{Instant, ZoneOffset}
  import java.time.format.DateTimeFormatter

  private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC)
  private val hourFormatter = DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC)

  override def getBucketId(
    element: PersonEvent,
    context: org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner.Context
  ): String =
    val instant = Instant.ofEpochMilli(element.eventTime)
    val date = dateFormatter.format(instant)
    val hour = hourFormatter.format(instant)
    s"dt=$date/hour=$hour"

  override def getSerializer: org.apache.flink.core.io.SimpleVersionedSerializer[String] =
    org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer.INSTANCE
