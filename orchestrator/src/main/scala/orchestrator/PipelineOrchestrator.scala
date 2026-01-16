package orchestrator

import zio.*
import scala.concurrent.duration.*
import scala.sys.process.*
import java.nio.file.Paths
import java.nio.file.Files

/**
 * Pure functional pipeline orchestrator using ZIO
 *
 * Orchestrates the complete data pipeline:
 * 1. Publish test events to Kafka
 * 2. Submit Flink job for stream processing
 * 3. Wait for Parquet output in data lake
 * 4. Run Spark ETL batch analytics
 *
 * All stages run sequentially with proper error handling
 */
object PipelineOrchestrator:

  /**
   * Execute a shell command and return the exit code
   */
  private def executeCommand(cmd: Seq[String]): ZIO[Any, Throwable, Int] =
    ZIO.attempt {
      Process(cmd).!
    }

  /**
   * Execute a shell command and return its output
   */
  private def executeCommandOutput(cmd: Seq[String]): ZIO[Any, Throwable, String] =
    ZIO.attempt {
      Process(cmd).!!
    }

  /**
   * Publish test events to Kafka
   * Runs: sbt "preprocessing/runMain preprocessing.PreprocessingPipeline publishTestDataToKafka 100"
   */
  def publishToKafka(eventCount: Int): ZIO[Any, Throwable, Int] =
    for
      _ <- ZIO.logInfo(s"📤 Publishing $eventCount events to Kafka...")

      // Run the preprocessing pipeline
      cmd = Seq("sbt", "preprocessing/runMain", "preprocessing.PreprocessingPipeline", "publishTestDataToKafka", eventCount.toString)
      result <- executeCommand(cmd)

      _ <- if result == 0 then
        ZIO.logInfo(s"✅ Successfully published $eventCount events to Kafka")
      else
        ZIO.fail(Exception(s"❌ Failed to publish events (exit code: $result)"))

    yield result

  /**
   * Build and submit Flink job
   * Builds JAR and submits to Flink via REST API
   */
  def submitFlinkJob(): ZIO[Any, Throwable, String] =
    for
      _ <- ZIO.logInfo("🔨 Building Flink job...")

      // Build assembly JAR
      buildResult <- executeCommand(Seq("sbt", "flinkStreaming/assembly"))
      _ <- if buildResult != 0 then
        ZIO.fail(Exception(s"Failed to build Flink job (exit code: $buildResult)"))
      else
        ZIO.logInfo("✅ Built Flink job JAR")

      _ <- ZIO.logInfo("📮 Submitting Flink job...")

      // Find the built JAR
      jarPath = "flink-streaming/target/scala-3.5.2/flink-streaming-assembly.jar"

      // Submit via Flink CLI
      cmd = Seq("flink", "run", "-c", "flink.StreamingJob", jarPath)
      submitResult <- executeCommandOutput(cmd).map(_.trim)

      jobId <- extractJobId(submitResult)
      _ <- ZIO.logInfo(s"✅ Submitted Flink job with ID: $jobId")

    yield jobId

  /**
   * Wait for Parquet data to appear in the data lake
   * Polls the output directory until files are created
   */
  def waitForParquetOutput(
    maxWaitSeconds: Int = 120,
    pollIntervalSeconds: Int = 5
  ): ZIO[Any, Throwable, String] =
    val outputPath = "data/streaming/people"

    ZIO.logInfo(s"⏳ Waiting for Parquet output in $outputPath (max ${maxWaitSeconds}s)...") *>
    checkParquetOutput(outputPath, maxWaitSeconds, pollIntervalSeconds)

  private def checkParquetOutput(
    path: String,
    remainingSeconds: Int,
    pollInterval: Int
  ): ZIO[Any, Throwable, String] =
    if remainingSeconds <= 0 then
      ZIO.fail(Exception(s"Timeout waiting for Parquet output in $path"))
    else
      val hasParquetFiles = ZIO.attempt {
        val dataPath = Paths.get(path)
        Files.exists(dataPath) && Files.list(dataPath).toArray().nonEmpty
      }

      for
        hasFiles <- hasParquetFiles.orElse(ZIO.succeed(false))
        result <- if hasFiles then
          ZIO.logInfo(s"✅ Found Parquet files in $path") *>
          ZIO.succeed(path)
        else
          ZIO.logInfo(s"⏳ No Parquet files yet, checking again in ${pollInterval}s...") *>
          ZIO.sleep(pollInterval.seconds) *>
          checkParquetOutput(path, remainingSeconds - pollInterval, pollInterval)
      yield result

  /**
   * Run Spark ETL batch analytics
   */
  def runSparkETL(): ZIO[Any, Throwable, Int] =
    for
      _ <- ZIO.logInfo("🚀 Running Spark ETL batch analytics...")

      // Build Spark ETL assembly
      buildResult <- executeCommand(Seq("sbt", "etl/assembly"))
      _ <- if buildResult != 0 then
        ZIO.fail(Exception(s"Failed to build Spark ETL (exit code: $buildResult)"))
      else
        ZIO.logInfo("✅ Built Spark ETL JAR")

      // Submit Spark job
      submitResult <- executeCommand(Seq(
        "spark-submit",
        "--class", "etl.SparkETLPipeline",
        "--master", "local[*]",
        "etl/target/scala-2.13/etl-assembly.jar"
      ))

      _ <- if submitResult == 0 then
        ZIO.logInfo("✅ Spark ETL completed successfully")
      else
        ZIO.fail(Exception(s"Spark ETL failed (exit code: $submitResult)"))

    yield submitResult

  /**
   * Complete end-to-end orchestration
   */
  def runPipeline(eventCount: Int = 100): ZIO[Any, Throwable, Unit] =
    for
      _ <- ZIO.logInfo("=" * 80)
      _ <- ZIO.logInfo("🎯 Starting Complete Data Pipeline Orchestration")
      _ <- ZIO.logInfo("=" * 80)

      // Stage 1: Publish to Kafka
      _ <- publishToKafka(eventCount)
      _ <- ZIO.sleep(2.seconds)

      // Stage 2: Submit Flink job
      jobId <- submitFlinkJob()
      _ <- ZIO.sleep(2.seconds)

      // Stage 3: Wait for Parquet output
      outputPath <- waitForParquetOutput()
      _ <- ZIO.sleep(2.seconds)

      // Stage 4: Run Spark ETL
      _ <- runSparkETL()

      _ <- ZIO.logInfo("=" * 80)
      _ <- ZIO.logInfo("✅ Pipeline orchestration completed successfully!")
      _ <- ZIO.logInfo("=" * 80)

    yield ()

  /**
   * Extract Job ID from Flink submit response
   * Flink returns: "Job has been submitted with JobID {jobid}"
   */
  private def extractJobId(output: String): ZIO[Any, Throwable, String] =
    val pattern = """JobID ([a-f0-9]+)""".r
    pattern.findFirstMatchIn(output) match
      case Some(m) => ZIO.succeed(m.group(1))
      case None =>
        ZIO.logWarning(s"Could not extract Job ID from: $output") *>
        ZIO.succeed("unknown")


/**
 * Main entry point for the orchestrator
 */
object OrchestratorMain extends ZIOAppDefault:
  def run =
    for
      args <- getArgs
      eventCount = args.headOption.flatMap(_.toIntOption).getOrElse(100)
      _ <- PipelineOrchestrator.runPipeline(eventCount)
    yield ()