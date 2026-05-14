package orchestrator

import zio.*
import preprocessing.KafkaPublisher
import preprocessing.models.Person
import preprocessing.models.types.*
import preprocessing.processors.FileOperations
import shared.config.Paths
import shared.schemas.PersonSchema

import java.nio.file.{Files, Path, Paths as JPaths}
import java.time.{LocalDateTime, LocalDate}
import java.time.format.DateTimeFormatter
import java.util.Locale

/** Resolve paths relative to project root (handles forked JVM in orchestrator/) */
private def projectRoot: Path =
  val cwd = JPaths.get("").toAbsolutePath
  if cwd.endsWith("orchestrator") then cwd.getParent else cwd

trait Task:
  def id: String
  def description: String
  def dependsOn: List[String] = List()
  def run: ZIO[Any, Throwable, TaskResult]
  def taskType: String
  def estimatedDuration: Int = 60

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
 * Task 1: Generate PersonEvent records and write to data lake as Parquet
 */
object GenerateEventsTask extends Task:
  override def id = "generate_events"
  override def description = "Generate PersonEvent records and write to data lake"
  override def dependsOn = List()
  override def taskType = "Producer"
  override def estimatedDuration = 10

  override def run: ZIO[Any, Throwable, TaskResult] =
    for
      start <- Clock.instant
      _ <- ZIO.logInfo("Generating PersonEvent records...")

      result <- ZIO.attemptBlocking {
        import cats.effect.unsafe.implicits.global

        TaskExecutionTracker.addLog(id, "Generating 500 Person records using preprocessing module", "INFO")
        val people = KafkaPublisher.generateTestPeople(500)
        TaskExecutionTracker.addLog(id, s"Generated ${people.size} Person records", "INFO")

        // Write to partitioned path with unique filename per run
        val now = LocalDateTime.now()
        val today = now.toLocalDate.toString
        val timestamp = now.format(DateTimeFormatter.ofPattern("HHmmss"))
        val partitionDir = projectRoot.resolve(s"data/streaming/people/dt=$today")
        Files.createDirectories(partitionDir)
        val outputFile = partitionDir.resolve(s"run-$timestamp.parquet")

        TaskExecutionTracker.addLog(id, s"Writing Parquet to $outputFile", "INFO")
        val fileOps = FileOperations()
        fileOps.writeParquet(outputFile, people).unsafeRunSync()

        TaskExecutionTracker.addLog(id, s"Successfully wrote ${people.size} records to data lake", "INFO")

        // Update tracker with real metrics
        TaskExecutionTracker.updateProgress(id, people.size.toLong)

        // Add sample data (first 5 records)
        val sampleRows = people.take(5).map { person =>
          Map(
            "name" -> person.name,
            "email" -> person.email.value,
            "age" -> person.age.value.toString,
            "city" -> person.city.value,
            "status" -> person.status.toStorageString
          )
        }
        TaskExecutionTracker.addSampleData(id, sampleRows)

        // Add task metadata
        TaskExecutionTracker.addTaskDataMap(id, Map(
          "Records Generated" -> people.size.toString,
          "Output Path" -> Paths.People.streamingParquet,
          "Format" -> "Parquet",
          "Unique Cities" -> people.map(_.city.value).distinct.size.toString,
          "Age Range" -> s"${people.map(_.age.value).min} - ${people.map(_.age.value).max}"
        ))

        people.size
      }

      end <- Clock.instant
      duration = end.toEpochMilli - start.toEpochMilli

    yield TaskResult(
      taskId = id,
      success = true,
      output = s"Generated and wrote $result PersonEvent records to data lake",
      duration = duration
    )

/**
 * Task 2: Validate data lake - read back Parquet and verify
 */
object ValidateDataLakeTask extends Task:
  override def id = "validate_data_lake"
  override def description = "Read and validate Parquet data from data lake"
  override def dependsOn = List("generate_events")
  override def taskType = "Validator"
  override def estimatedDuration = 8

  override def run: ZIO[Any, Throwable, TaskResult] =
    for
      start <- Clock.instant
      _ <- ZIO.logInfo("Validating data lake...")

      result <- ZIO.attemptBlocking {
        import cats.effect.unsafe.implicits.global

        val dataPath = projectRoot.resolve("data/streaming/people")
        TaskExecutionTracker.addLog(id, s"Scanning data lake at: $dataPath", "INFO")

        // Check for Parquet files
        val parquetFiles = if Files.exists(dataPath) then
          Files.walk(dataPath)
            .filter(p => p.toString.endsWith(".parquet"))
            .toArray
            .map(_.asInstanceOf[Path])
            .toList
        else List.empty

        val totalSize = parquetFiles.map(f => Files.size(f)).sum
        TaskExecutionTracker.addLog(id, s"Found ${parquetFiles.size} Parquet file(s), total size: ${totalSize / 1024}KB", "INFO")

        // Read back records from each Parquet file
        TaskExecutionTracker.addLog(id, "Reading Parquet records for validation...", "INFO")
        val fileOps = FileOperations()
        val people = parquetFiles.flatMap { pf =>
          fileOps.readParquet(pf).unsafeRunSync()
        }

        TaskExecutionTracker.addLog(id, s"Read ${people.size} records from data lake", "INFO")

        // Validate schema fields
        val validCount = people.count(p => p.name.nonEmpty && p.age.value >= 0)
        val invalidCount = people.size - validCount
        TaskExecutionTracker.addLog(id, s"Validation: $validCount valid, $invalidCount invalid", if invalidCount > 0 then "WARN" else "INFO")

        TaskExecutionTracker.updateProgress(id, people.size.toLong, invalidCount.toLong)

        // Sample data from read-back
        val sampleRows = people.take(5).map { person =>
          Map(
            "name" -> person.name,
            "email" -> person.email.value,
            "age" -> person.age.value.toString,
            "city" -> person.city.value,
            "status" -> person.status.toStorageString
          )
        }.toList
        TaskExecutionTracker.addSampleData(id, sampleRows)

        TaskExecutionTracker.addTaskDataMap(id, Map(
          "Parquet Files" -> parquetFiles.size.toString,
          "Total Size" -> s"${totalSize / 1024}KB",
          "Records Read" -> people.size.toString,
          "Valid Records" -> validCount.toString,
          "Invalid Records" -> invalidCount.toString,
          "Data Path" -> dataPath.toString
        ))

        (people.size, validCount)
      }

      end <- Clock.instant
      duration = end.toEpochMilli - start.toEpochMilli

    yield TaskResult(
      taskId = id,
      success = true,
      output = s"Validated ${result._1} records (${result._2} valid)",
      duration = duration
    )

/**
 * Task 3: Compute analytics - mirrors SparkETLPipeline logic in pure Scala
 */
object ComputeAnalyticsTask extends Task:
  override def id = "compute_analytics"
  override def description = "Compute batch analytics on data lake records"
  override def dependsOn = List("validate_data_lake")
  override def taskType = "Analyzer"
  override def estimatedDuration = 12

  override def run: ZIO[Any, Throwable, TaskResult] =
    for
      start <- Clock.instant
      _ <- ZIO.logInfo("Computing analytics...")

      result <- ZIO.attemptBlocking {
        import cats.effect.unsafe.implicits.global

        val dataPath = projectRoot.resolve("data/streaming/people")
        TaskExecutionTracker.addLog(id, "Reading data lake for analytics...", "INFO")

        val fileOps = FileOperations()
        val parquetFiles = if Files.exists(dataPath) then
          Files.walk(dataPath)
            .filter(p => p.toString.endsWith(".parquet"))
            .toArray
            .map(_.asInstanceOf[Path])
            .toList
        else List.empty
        val people = parquetFiles.flatMap(pf => fileOps.readParquet(pf).unsafeRunSync())
        TaskExecutionTracker.addLog(id, s"Loaded ${people.size} records for analysis", "INFO")

        // City statistics (mirrors SparkETLPipeline.computeStatistics)
        TaskExecutionTracker.addLog(id, "Computing city statistics...", "INFO")
        val cityStats = people.groupBy(_.city.value).map { case (city, group) =>
          val ages = group.map(_.age.value)
          Map(
            "city" -> city,
            "total_people" -> group.size.toString,
            "avg_age" -> String.format(Locale.US, "%.1f", ages.sum.toDouble / ages.size),
            "min_age" -> ages.min.toString,
            "max_age" -> ages.max.toString
          )
        }.toList.sortBy(m => -m("total_people").toInt)

        // Age group distribution (mirrors SparkETLPipeline.enrichData)
        TaskExecutionTracker.addLog(id, "Computing age group distribution...", "INFO")
        val ageGroups = people.groupBy { person =>
          val age = person.age.value
          if age < 18 then "Minor"
          else if age < 30 then "Young Adult"
          else if age < 50 then "Adult"
          else if age < 65 then "Middle Age"
          else "Senior"
        }.map { case (group, persons) =>
          Map("age_group" -> group, "count" -> persons.size.toString, "percentage" -> String.format(Locale.US, "%.1f%%", persons.size.toDouble / people.size * 100))
        }.toList.sortBy(m => -m("count").toInt)

        // Status distribution
        val statusDist = people.groupBy(_.status.toStorageString).map { case (status, persons) =>
          (status, persons.size)
        }

        // Email domain distribution
        val domainDist = people.groupBy(_.email.value.split("@").lastOption.getOrElse("unknown")).map { case (domain, persons) =>
          (domain, persons.size)
        }

        // Write results as JSON
        val outputDir = projectRoot.resolve("data/output")
        Files.createDirectories(outputDir)
        val outputPath = outputDir.resolve("analytics.json")

        val jsonContent = buildAnalyticsJson(cityStats, ageGroups, statusDist.toMap, domainDist.toMap, people.size)
        Files.writeString(outputPath, jsonContent)
        TaskExecutionTracker.addLog(id, s"Wrote analytics to ${outputPath}", "INFO")

        TaskExecutionTracker.updateProgress(id, people.size.toLong)

        // Show city stats as sample data
        TaskExecutionTracker.addSampleData(id, cityStats)

        TaskExecutionTracker.addTaskDataMap(id, Map(
          "Total Records Analyzed" -> people.size.toString,
          "Unique Cities" -> cityStats.size.toString,
          "Age Groups" -> ageGroups.map(m => s"${m("age_group")}: ${m("count")}").mkString(", "),
          "Status Distribution" -> statusDist.map((s, c) => s"$s: $c").mkString(", "),
          "Output File" -> outputPath.toString
        ))

        people.size
      }

      end <- Clock.instant
      duration = end.toEpochMilli - start.toEpochMilli

    yield TaskResult(
      taskId = id,
      success = true,
      output = s"Computed analytics on $result records",
      duration = duration
    )

  private def buildAnalyticsJson(
    cityStats: List[Map[String, String]],
    ageGroups: List[Map[String, String]],
    statusDist: Map[String, Int],
    domainDist: Map[String, Int],
    totalRecords: Int
  ): String =
    val cityJson = cityStats.map { m =>
      s"""    {"city":"${m("city")}","total_people":${m("total_people")},"avg_age":${m("avg_age")},"min_age":${m("min_age")},"max_age":${m("max_age")}}"""
    }.mkString(",\n")

    val ageJson = ageGroups.map { m =>
      s"""    {"age_group":"${m("age_group")}","count":${m("count")},"percentage":"${m("percentage")}"}"""
    }.mkString(",\n")

    val statusJson = statusDist.map((s, c) => s"""    "$s": $c""").mkString(",\n")
    val domainJson = domainDist.map((d, c) => s"""    "$d": $c""").mkString(",\n")

    s"""{
       |  "generated_at": "${LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)}",
       |  "total_records": $totalRecords,
       |  "city_statistics": [
       |$cityJson
       |  ],
       |  "age_groups": [
       |$ageJson
       |  ],
       |  "status_distribution": {
       |$statusJson
       |  },
       |  "email_domains": {
       |$domainJson
       |  }
       |}""".stripMargin

/**
 * Task 4: Export results - read analytics and display summary
 */
object ExportResultsTask extends Task:
  override def id = "export_results"
  override def description = "Export analytics results and generate summary"
  override def dependsOn = List("compute_analytics")
  override def taskType = "Reporter"
  override def estimatedDuration = 5

  override def run: ZIO[Any, Throwable, TaskResult] =
    for
      start <- Clock.instant
      _ <- ZIO.logInfo("Exporting results...")

      result <- ZIO.attemptBlocking {
        import cats.effect.unsafe.implicits.global

        val outputPath = projectRoot.resolve("data/output/analytics.json")
        TaskExecutionTracker.addLog(id, s"Reading analytics from $outputPath", "INFO")

        val jsonContent = Files.readString(outputPath)
        val totalRecords = extractJsonInt(jsonContent, "total_records")

        // Build run history from all parquet files in data lake
        val dataPath = projectRoot.resolve("data/streaming/people")
        TaskExecutionTracker.addLog(id, "Scanning data lake for run history...", "INFO")

        val fileOps = FileOperations()
        val parquetFiles = if Files.exists(dataPath) then
          Files.walk(dataPath)
            .filter(p => p.toString.endsWith(".parquet"))
            .toArray
            .map(_.asInstanceOf[Path])
            .toList
            .sortBy(p => Files.getLastModifiedTime(p).toInstant)
        else List.empty

        // Read each file and build run history with timestamps
        var runningTotal = 0
        val runHistory = parquetFiles.map { pf =>
          val recordCount = fileOps.readParquet(pf).unsafeRunSync().size
          runningTotal += recordCount
          val fileSize = Files.size(pf)
          val modified = Files.getLastModifiedTime(pf).toInstant
            .atZone(java.time.ZoneId.systemDefault())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

          TaskExecutionTracker.addLog(id, s"  $modified: $recordCount records (cumulative: $runningTotal)", "INFO")

          Map(
            "timestamp" -> modified,
            "records" -> recordCount.toString,
            "cumulative_total" -> runningTotal.toString,
            "size" -> s"${fileSize / 1024}KB"
          )
        }

        TaskExecutionTracker.addLog(id, s"Total across ${parquetFiles.size} run(s): $runningTotal records", "INFO")

        // Show run history as sample data
        TaskExecutionTracker.addSampleData(id, runHistory)
        TaskExecutionTracker.updateProgress(id, totalRecords.toLong)

        val dataSize = if Files.exists(dataPath) then
          Files.walk(dataPath).filter(p => Files.isRegularFile(p)).mapToLong(p => Files.size(p)).sum()
        else 0L

        TaskExecutionTracker.addTaskDataMap(id, Map(
          "Analytics File" -> outputPath.toString,
          "Total Records" -> totalRecords.toString,
          "Runs" -> parquetFiles.size.toString,
          "Data Lake Size" -> s"${dataSize / 1024}KB",
          "Pipeline Status" -> "Complete"
        ))

        TaskExecutionTracker.addLog(id, "Pipeline complete - all results exported", "INFO")

        totalRecords
      }

      end <- Clock.instant
      duration = end.toEpochMilli - start.toEpochMilli

    yield TaskResult(
      taskId = id,
      success = true,
      output = s"Exported analytics for $result records",
      duration = duration
    )

  private def extractJsonInt(json: String, key: String): Int =
    val pattern = s""""$key"\\s*:\\s*(\\d+)""".r
    pattern.findFirstMatchIn(json).map(_.group(1).toInt).getOrElse(0)

  private def extractCityStats(json: String): List[Map[String, String]] =
    val cityPattern = """"city"\s*:\s*"([^"]+)"\s*,\s*"total_people"\s*:\s*(\d+)\s*,\s*"avg_age"\s*:\s*([\d.,]+)\s*,\s*"min_age"\s*:\s*(\d+)\s*,\s*"max_age"\s*:\s*(\d+)""".r
    cityPattern.findAllMatchIn(json).map { m =>
      Map(
        "city" -> m.group(1),
        "total_people" -> m.group(2),
        "avg_age" -> m.group(3),
        "min_age" -> m.group(4),
        "max_age" -> m.group(5)
      )
    }.toList

  private def extractAgeGroups(json: String): List[Map[String, String]] =
    val agePattern = """"age_group"\s*:\s*"([^"]+)"\s*,\s*"count"\s*:\s*(\d+)\s*,\s*"percentage"\s*:\s*"([^"]+)"""".r
    agePattern.findAllMatchIn(json).map { m =>
      Map(
        "age_group" -> m.group(1),
        "count" -> m.group(2),
        "percentage" -> m.group(3)
      )
    }.toList

/**
 * All available tasks
 */
object AllTasks:
  val tasks: List[Task] = List(
    GenerateEventsTask,
    ValidateDataLakeTask,
    ComputeAnalyticsTask,
    ExportResultsTask
  )

  def getTask(taskId: String): Option[Task] =
    tasks.find(_.id == taskId)

  def allTaskIds: List[String] =
    tasks.map(_.id)

  def getTasksInOrder: List[Task] =
    def sort(remaining: List[Task], sorted: List[Task]): List[Task] =
      if remaining.isEmpty then sorted
      else
        val next = remaining.find(t => t.dependsOn.forall(dep => sorted.exists(_.id == dep)))
        next match
          case Some(task) => sort(remaining.filterNot(_ == task), sorted :+ task)
          case None => sorted

    sort(tasks, List())
