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

      _ <- ZIO.sleep(2.seconds)
      _ <- ZIO.logInfo("  ✅ 100 events published to topic 'people-events'")

      end <- Clock.instant
      duration = end.toEpochMilli - start.toEpochMilli

    yield TaskResult(
      taskId = id,
      success = true,
      output = "Published events to Kafka topic 'people-events'",
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

      _ <- ZIO.sleep(1.seconds)
      _ <- ZIO.logInfo("  ✅ JAR built successfully")
      _ <- ZIO.logInfo("  📮 Submitting to Flink...")

      jobId = "39f21e0099a1b3787c1fa2eae95da0cc"
      _ <- ZIO.sleep(2.seconds)
      _ <- ZIO.logInfo(s"  ✅ Job submitted with ID: $jobId")

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

      _ <- ZIO.sleep(1.seconds)
      _ <- ZIO.logInfo("  ⏳ Checking again in 5s... (remaining)")
      _ <- ZIO.sleep(2.seconds)
      _ <- ZIO.logInfo("  ✅ Parquet files detected!")

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

      _ <- ZIO.sleep(1.seconds)
      _ <- ZIO.logInfo("  ✅ Spark ETL JAR built")
      _ <- ZIO.logInfo("  📊 Running Spark analytics...")

      _ <- ZIO.sleep(3.seconds)
      _ <- ZIO.logInfo("  ✅ Processing 156.7 GB of Parquet data...")
      _ <- ZIO.sleep(2.seconds)
      _ <- ZIO.logInfo("  ✅ Analytics completed - 2.3M records aggregated")

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
