package shared.kafka

/**
 * Kafka configuration constants
 * Shared across producer (preprocessing) and consumer (flink-streaming)
 */
object KafkaConfig {
  // Topic names
  val PEOPLE_TOPIC = "people-events"

  // Consumer group IDs
  val FLINK_CONSUMER_GROUP = "flink-streaming-consumer"

  // Bootstrap servers (environment-aware)
  def bootstrapServers: String =
    sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

  // Producer configuration
  object Producer {
    val ACKS = "all" // Wait for all replicas to acknowledge
    val RETRIES = 3
    val BATCH_SIZE = 16384
    val LINGER_MS = 10 // Wait up to 10ms to batch messages
    val BUFFER_MEMORY = 33554432L // 32MB buffer
    val COMPRESSION_TYPE = "snappy"
  }

  // Consumer configuration
  object Consumer {
    val AUTO_OFFSET_RESET = "earliest"
    val ENABLE_AUTO_COMMIT = false // Manual commit for exactly-once semantics
    val SESSION_TIMEOUT_MS = 30000
    val MAX_POLL_RECORDS = 500
  }

  // Flink-specific configuration
  object Flink {
    val CHECKPOINT_INTERVAL_MS = 60000L // 1 minute checkpoints
    val MIN_PAUSE_BETWEEN_CHECKPOINTS = 5000L // 5 seconds
    val CHECKPOINT_TIMEOUT_MS = 300000L // 5 minutes
  }
}
