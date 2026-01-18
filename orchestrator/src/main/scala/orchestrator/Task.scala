package orchestrator

import zio.*

/**
 * Task abstraction - similar to Airflow operators
 *
 * Each task represents a single unit of work that can be:
 * - Executed independently
 * - Part of a DAG with dependencies
 * - Retried on failure
 * - Monitored and logged
 */
trait Task:
  /**
   * Unique task ID
   */
  def id: String

  /**
   * Human-readable description
   */
  def description: String

  /**
   * Task dependencies (other task IDs)
   */
  def dependsOn: List[String] = List()

  /**
   * Execute the task
   * Returns: (success: Boolean, output: String, error: Option[String])
   */
  def run: ZIO[Any, Throwable, TaskResult]

  /**
   * Task type for categorization
   */
  def taskType: String

  /**
   * Estimated duration in seconds
   */
  def estimatedDuration: Int = 60

/**
 * Result of task execution
 */
case class TaskResult(
  taskId: String,
  success: Boolean,
  output: String,
  error: Option[String] = None,
  duration: Long = 0
):
  def toJson: String =
    s"""{"taskId":"$taskId","success":$success,"output":${escapeJson(output)},"error":${error.map(e => s""""${escapeJson(e)}"""").getOrElse("null")},"duration":$duration}"""

  private def escapeJson(str: String): String =
    "\"" + str.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + "\""

/**
 * Task 1: PublishToKafka (Test Implementation)
 */
object PublishToKafkaTask extends Task:
  override def id = "publish_to_kafka"
  override def description = "Generate and publish events to Kafka"
  override def dependsOn = List()
  override def taskType = "Producer"
  override def estimatedDuration = 8

  override def run: ZIO[Any, Throwable, TaskResult] =
    for
      start <- Clock.instant
      _ <- ZIO.logInfo("🚀 Task: PublishToKafka")
      _ <- ZIO.logInfo("  Generating 100 test events...")

      // Simulate event generation and publishing
      eventsGenerated = 100
      sampleRecords = List(
        Map("id" -> "1", "name" -> "John Doe", "email" -> "john@example.com", "age" -> "28", "city" -> "NYC", "status" -> "active", "createdAt" -> "2026-01-17T12:00:00Z"),
        Map("id" -> "2", "name" -> "Jane Smith", "email" -> "jane@example.com", "age" -> "34", "city" -> "LA", "status" -> "active", "createdAt" -> "2026-01-17T12:00:01Z"),
        Map("id" -> "3", "name" -> "Bob Johnson", "email" -> "bob@example.com", "age" -> "45", "city" -> "Chicago", "status" -> "inactive", "createdAt" -> "2026-01-17T12:00:02Z"),
        Map("id" -> "4", "name" -> "Alice Williams", "email" -> "alice@example.com", "age" -> "29", "city" -> "Boston", "status" -> "active", "createdAt" -> "2026-01-17T12:00:03Z"),
        Map("id" -> "5", "name" -> "Charlie Brown", "email" -> "charlie@example.com", "age" -> "52", "city" -> "Seattle", "status" -> "inactive", "createdAt" -> "2026-01-17T12:00:04Z")
      )

      _ <- ZIO.succeed {
        TaskExecutionTracker.addTaskDataMap(id, Map(
          "Topic" -> "people-events",
          "Events Generated" -> eventsGenerated.toString,
          "Bootstrap Servers" -> "localhost:9092",
          "Format" -> "JSON",
          "Status" -> "Published to Kafka",
          "Timestamp" -> java.time.Instant.now().toString
        ))
        TaskExecutionTracker.addSampleData(id, sampleRecords)
      }

      // Simulate processing time
      _ <- ZIO.sleep(2.seconds)
      _ <- ZIO.logInfo("  ✅ 100 events published to topic 'people-events'")

      end <- Clock.instant
      duration = end.toEpochMilli - start.toEpochMilli

    yield TaskResult(
      taskId = id,
      success = true,
      output = s"Published $eventsGenerated events to Kafka topic 'people-events'",
      error = None,
      duration = duration
    )

/**
 * Task 2: SubmitFlinkJob (Test Implementation)
 */
object SubmitFlinkJobTask extends Task:
  override def id = "submit_flink_job"
  override def description = "Build and submit Flink streaming job"
  override def dependsOn = List("publish_to_kafka")
  override def taskType = "Processor"
  override def estimatedDuration = 33

  override def run: ZIO[Any, Throwable, TaskResult] =
    for
      start <- Clock.instant
      _ <- ZIO.logInfo("🚀 Task: SubmitFlinkJob")
      _ <- ZIO.logInfo("  Building Flink assembly JAR...")

      // Simulate JAR build
      _ <- ZIO.sleep(1.seconds)
      _ <- ZIO.logInfo("  ✅ JAR built successfully")
      _ <- ZIO.logInfo("  📮 Submitting to Flink...")

      // Generate realistic job ID
      jobId = "39f21e0099a1b3787c1fa2eae95da0cc"
      _ <- ZIO.sleep(2.seconds)
      _ <- ZIO.logInfo(s"  ✅ Job submitted with ID: $jobId")

      sampleRecords = List(
        Map("taskId" -> "Flink-StreamingTask-1", "slot" -> "1", "parallelism" -> "4", "status" -> "RUNNING", "backpressure" -> "OK", "checkpoint" -> "1847"),
        Map("taskId" -> "Flink-StreamingTask-2", "slot" -> "2", "parallelism" -> "4", "status" -> "RUNNING", "backpressure" -> "OK", "checkpoint" -> "1847"),
        Map("taskId" -> "Flink-SinkTask-1", "slot" -> "3", "parallelism" -> "2", "status" -> "RUNNING", "backpressure" -> "LOW", "checkpoint" -> "1847"),
        Map("taskId" -> "Flink-SinkTask-2", "slot" -> "4", "parallelism" -> "2", "status" -> "RUNNING", "backpressure" -> "LOW", "checkpoint" -> "1847"),
        Map("taskId" -> "Flink-CheckpointCoordinator", "slot" -> "-", "parallelism" -> "1", "status" -> "RUNNING", "backpressure" -> "OK", "checkpoint" -> "1847")
      )

      _ <- ZIO.succeed {
        TaskExecutionTracker.addTaskDataMap(id, Map(
          "JAR Path" -> "flink-streaming/target/scala-3.5.2/flink-streaming-assembly.jar",
          "Job ID" -> jobId,
          "Kafka Topic" -> "people-events",
          "Output Format" -> "Parquet",
          "Partitioning" -> "dt=YYYY-MM-DD/hour=HH",
          "Status" -> "Running",
          "Parallelism" -> "4"
        ))
        TaskExecutionTracker.addSampleData(id, sampleRecords)
      }

      end <- Clock.instant
      duration = end.toEpochMilli - start.toEpochMilli

    yield TaskResult(
      taskId = id,
      success = true,
      output = s"Flink job submitted with ID: $jobId",
      error = None,
      duration = duration
    )

/**
 * Task 3: WaitForParquetOutput (Test Implementation)
 */
object WaitForParquetOutputTask extends Task:
  override def id = "wait_for_parquet"
  override def description = "Wait for Parquet files to appear in data lake"
  override def dependsOn = List("submit_flink_job")
  override def taskType = "Sensor"
  override def estimatedDuration = 30

  override def run: ZIO[Any, Throwable, TaskResult] =
    for
      start <- Clock.instant
      _ <- ZIO.logInfo("🚀 Task: WaitForParquetOutput")
      _ <- ZIO.logInfo("  Polling data/streaming/people/ for Parquet files...")

      // Simulate checking for files
      _ <- ZIO.sleep(1.seconds)
      _ <- ZIO.logInfo("  ⏳ Checking again in 5s... (remaining)")
      _ <- ZIO.sleep(2.seconds)
      _ <- ZIO.logInfo("  ✅ Parquet files detected!")

      sampleRecords = List(
        Map("date" -> "2026-01-17", "hour" -> "00", "files" -> "512", "size_mb" -> "45.8", "status" -> "COMPLETE"),
        Map("date" -> "2026-01-17", "hour" -> "01", "files" -> "521", "size_mb" -> "47.2", "status" -> "COMPLETE"),
        Map("date" -> "2026-01-17", "hour" -> "02", "files" -> "508", "size_mb" -> "44.9", "status" -> "COMPLETE"),
        Map("date" -> "2026-01-17", "hour" -> "03", "files" -> "495", "size_mb" -> "43.1", "status" -> "COMPLETE"),
        Map("date" -> "2026-01-17", "hour" -> "04", "files" -> "519", "size_mb" -> "46.5", "status" -> "COMPLETE")
      )

      _ <- ZIO.succeed {
        TaskExecutionTracker.addTaskDataMap(id, Map(
          "Data Lake Path" -> "data/streaming/people/",
          "Status" -> "Files Detected",
          "Format" -> "Parquet",
          "Partitioning" -> "dt=YYYY-MM-DD/hour=HH",
          "Retention" -> "7 days",
          "File Count" -> "2847",
          "Total Size" -> "156.7 GB"
        ))
        TaskExecutionTracker.addSampleData(id, sampleRecords)
      }

      end <- Clock.instant
      duration = end.toEpochMilli - start.toEpochMilli

    yield TaskResult(
      taskId = id,
      success = true,
      output = "Parquet files detected in data lake",
      error = None,
      duration = duration
    )

/**
 * Task 4: RunSparkETL (Test Implementation)
 */
object RunSparkETLTask extends Task:
  override def id = "run_spark_etl"
  override def description = "Run Spark batch ETL analytics"
  override def dependsOn = List("wait_for_parquet")
  override def taskType = "Analyzer"
  override def estimatedDuration = 55

  override def run: ZIO[Any, Throwable, TaskResult] =
    for
      start <- Clock.instant
      _ <- ZIO.logInfo("🚀 Task: RunSparkETL")
      _ <- ZIO.logInfo("  Building Spark ETL assembly...")

      // Simulate JAR build
      _ <- ZIO.sleep(1.seconds)
      _ <- ZIO.logInfo("  ✅ Spark ETL JAR built")
      _ <- ZIO.logInfo("  📊 Running Spark analytics...")

      // Simulate Spark processing
      _ <- ZIO.sleep(3.seconds)
      _ <- ZIO.logInfo("  ✅ Processing 156.7 GB of Parquet data...")
      _ <- ZIO.sleep(2.seconds)
      _ <- ZIO.logInfo("  ✅ Analytics completed - 2.3M records aggregated")

      sampleRecords = List(
        Map("city" -> "NYC", "count" -> "347821", "avg_age" -> "31.2", "min_age" -> "18", "max_age" -> "75", "active_pct" -> "60%"),
        Map("city" -> "LA", "count" -> "278546", "avg_age" -> "29.5", "min_age" -> "21", "max_age" -> "68", "active_pct" -> "72%"),
        Map("city" -> "Chicago", "count" -> "189654", "avg_age" -> "33.1", "min_age" -> "25", "max_age" -> "71", "active_pct" -> "55%"),
        Map("city" -> "Boston", "count" -> "156432", "avg_age" -> "32.4", "min_age" -> "20", "max_age" -> "73", "active_pct" -> "68%"),
        Map("city" -> "Seattle", "count" -> "241368", "avg_age" -> "30.8", "min_age" -> "19", "max_age" -> "70", "active_pct" -> "65%")
      )

      _ <- ZIO.succeed {
        TaskExecutionTracker.addTaskDataMap(id, Map(
          "JAR Path" -> "etl/target/scala-2.13/etl-assembly.jar",
          "Input Path" -> "data/streaming/people/",
          "Output Path" -> "data/output/",
          "Processing" -> "Aggregations & Analytics",
          "Scala Version" -> "2.13.12",
          "Records Processed" -> "2,347,821",
          "Execution Time" -> "6.2s"
        ))
        TaskExecutionTracker.addSampleData(id, sampleRecords)
      }

      end <- Clock.instant
      duration = end.toEpochMilli - start.toEpochMilli

    yield TaskResult(
      taskId = id,
      success = true,
      output = "Spark ETL completed successfully - 2.3M records processed",
      error = None,
      duration = duration
    )

/**
 * All available tasks
 */
object AllTasks:
  val tasks: List[Task] = List(
    PublishToKafkaTask,
    SubmitFlinkJobTask,
    WaitForParquetOutputTask,
    RunSparkETLTask
  )

  def getTask(taskId: String): Option[Task] =
    tasks.find(_.id == taskId)

  def allTaskIds: List[String] =
    tasks.map(_.id)

  def getTasksInOrder: List[Task] =
    // Topological sort
    def sort(remaining: List[Task], sorted: List[Task]): List[Task] =
      if remaining.isEmpty then sorted
      else
        val next = remaining.find(t => t.dependsOn.forall(dep => sorted.exists(_.id == dep)))
        next match
          case Some(task) => sort(remaining.filterNot(_ == task), sorted :+ task)
          case None => sorted

    sort(tasks, List())
