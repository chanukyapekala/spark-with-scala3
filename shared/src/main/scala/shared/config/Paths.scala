package shared.config

/**
 * Centralized path configuration
 * Used by both preprocessing (Scala 3) and ETL (Scala 2.13) modules
 */
object Paths {
  // Local development paths
  val LOCAL_RAW_DATA = "data/raw/"
  val LOCAL_STREAMING_DATA = "data/streaming/"  // Flink writes partitioned data here
  val LOCAL_PROCESSED_DATA = "data/processed/"  // DEPRECATED: Legacy batch preprocessing (not used in pipeline)
  val LOCAL_OUTPUT_DATA = "data/output/"

  // S3/HDFS paths for production
  val S3_BUCKET = sys.env.getOrElse("S3_BUCKET", "s3://spark-with-scala3")
  val S3_RAW_DATA = s"$S3_BUCKET/raw/"
  val S3_STREAMING_DATA = s"$S3_BUCKET/streaming/"
  val S3_PROCESSED_DATA = s"$S3_BUCKET/processed/"  // DEPRECATED: Not used in current pipeline
  val S3_OUTPUT_DATA = s"$S3_BUCKET/output/"

  // Get paths based on environment
  def isLocal: Boolean = sys.env.getOrElse("ENV", "local") == "local"

  def rawData: String = if (isLocal) LOCAL_RAW_DATA else S3_RAW_DATA
  def streamingData: String = if (isLocal) LOCAL_STREAMING_DATA else S3_STREAMING_DATA
  def processedData: String = if (isLocal) LOCAL_PROCESSED_DATA else S3_PROCESSED_DATA
  def outputData: String = if (isLocal) LOCAL_OUTPUT_DATA else S3_OUTPUT_DATA

  // Specific data paths
  object People {
    def rawJson: String = s"${rawData}people.json"
    def rawJsonLines: String = s"${rawData}people.jsonl"
    def rawCsv: String = s"${rawData}people.csv"
    def streamingParquet: String = s"${streamingData}people"  // Flink writes partitioned data here (dt=YYYY-MM-DD/hour=HH)
    @deprecated("Use streamingParquet instead", since = "1.0")
    def processedParquet: String = s"${processedData}people.parquet"
    def outputStats: String = s"${outputData}people_stats.parquet"  // Spark ETL writes analytics results here
  }
}