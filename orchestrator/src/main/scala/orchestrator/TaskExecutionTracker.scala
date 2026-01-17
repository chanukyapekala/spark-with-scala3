package orchestrator

import zio.*
import scala.collection.mutable
import java.time.Instant

/**
 * Tracks task execution with real-time logs, metrics, and history
 */
object TaskExecutionTracker:

  case class ExecutionLog(
    timestamp: String,
    message: String,
    level: String = "INFO"  // INFO, WARN, ERROR
  )

  case class ExecutionMetrics(
    taskId: String,
    status: String,  // PENDING, RUNNING, COMPLETED, FAILED
    startTime: Option[String] = None,
    endTime: Option[String] = None,
    duration: Long = 0,
    recordsProcessed: Long = 0,
    recordsFailed: Long = 0,
    logs: List[ExecutionLog] = List(),
    error: Option[String] = None,
    taskData: Map[String, String] = Map()  // Task-specific output data
  ):
    def toJson: String =
      val taskDataJson = taskData.map { case (k, v) =>
        s""""$k":"${v.replace("\"", "\\\"")}""""
      }.mkString(",")
      s"""{
         |  "taskId": "$taskId",
         |  "status": "$status",
         |  "startTime": ${startTime.map(t => s""""$t"""").getOrElse("null")},
         |  "endTime": ${endTime.map(t => s""""$t"""").getOrElse("null")},
         |  "duration": $duration,
         |  "recordsProcessed": $recordsProcessed,
         |  "recordsFailed": $recordsFailed,
         |  "logs": [${logs.map(l =>
           s"""{"timestamp":"${l.timestamp}","message":"${l.message}","level":"${l.level}"}"""
         ).mkString(",")}],
         |  "taskData": {$taskDataJson},
         |  "error": ${error.map(e => s""""$e"""").getOrElse("null")}
         |}""".stripMargin

  // In-memory execution history
  private val executions = mutable.Map[String, ExecutionMetrics]()
  private val executionLocks = mutable.Map[String, Ref[ExecutionMetrics]]()

  def initializeTask(taskId: String): Unit =
    val metrics = ExecutionMetrics(
      taskId = taskId,
      status = "PENDING"
    )
    executions(taskId) = metrics

  def startTask(taskId: String): Unit =
    val now = Instant.now().toString
    executions.get(taskId).foreach { metrics =>
      executions(taskId) = metrics.copy(
        status = "RUNNING",
        startTime = Some(now),
        logs = metrics.logs :+ ExecutionLog(now, s"Task started", "INFO")
      )
    }

  def addLog(taskId: String, message: String, level: String = "INFO"): Unit =
    val now = Instant.now().toString
    executions.get(taskId).foreach { metrics =>
      executions(taskId) = metrics.copy(
        logs = metrics.logs :+ ExecutionLog(now, message, level)
      )
    }

  def updateProgress(taskId: String, recordsProcessed: Long, recordsFailed: Long = 0): Unit =
    executions.get(taskId).foreach { metrics =>
      executions(taskId) = metrics.copy(
        recordsProcessed = recordsProcessed,
        recordsFailed = recordsFailed
      )
    }

  def addTaskData(taskId: String, key: String, value: String): Unit =
    executions.get(taskId).foreach { metrics =>
      executions(taskId) = metrics.copy(
        taskData = metrics.taskData + (key -> value)
      )
    }

  def addTaskDataMap(taskId: String, data: Map[String, String]): Unit =
    executions.get(taskId).foreach { metrics =>
      executions(taskId) = metrics.copy(
        taskData = metrics.taskData ++ data
      )
    }

  def completeTask(taskId: String, success: Boolean, error: Option[String] = None): Unit =
    val now = Instant.now().toString
    executions.get(taskId).foreach { metrics =>
      val startTime = metrics.startTime.getOrElse(now)
      val duration = if metrics.startTime.isDefined then
        try
          val start = Instant.parse(startTime)
          val end = Instant.parse(now)
          (end.toEpochMilli - start.toEpochMilli)
        catch
          case _ => 0
      else 0

      val status = if success then "COMPLETED" else "FAILED"
      val logMsg = if success then "Task completed successfully" else s"Task failed: ${error.getOrElse("Unknown error")}"
      val logLevel = if success then "INFO" else "ERROR"

      executions(taskId) = metrics.copy(
        status = status,
        endTime = Some(now),
        duration = duration,
        logs = metrics.logs :+ ExecutionLog(now, logMsg, logLevel),
        error = error
      )
    }

  def getMetrics(taskId: String): Option[ExecutionMetrics] =
    executions.get(taskId)

  def getAllMetrics: Map[String, ExecutionMetrics] =
    executions.toMap

  def resetAll: Unit =
    executions.clear()
    executionLocks.clear()

  def getExecutionSummary: String =
    val metrics = executions.values.toList
    val completed = metrics.count(_.status == "COMPLETED")
    val failed = metrics.count(_.status == "FAILED")
    val running = metrics.count(_.status == "RUNNING")
    val pending = metrics.count(_.status == "PENDING")
    val totalDuration = metrics.map(_.duration).sum

    s"""{
       |  "total": ${metrics.length},
       |  "completed": $completed,
       |  "failed": $failed,
       |  "running": $running,
       |  "pending": $pending,
       |  "totalDuration": $totalDuration,
       |  "totalRecords": ${metrics.map(_.recordsProcessed).sum},
       |  "totalFailed": ${metrics.map(_.recordsFailed).sum}
       |}""".stripMargin