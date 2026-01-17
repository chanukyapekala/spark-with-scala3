package orchestrator

import zio.*

/**
 * Task Runner CLI - Similar to Airflow CLI but pure Scala/ZIO
 *
 * Commands:
 *   list              - List all tasks
 *   info <taskId>     - Show task information
 *   dag               - Show DAG visualization (table format)
 *   dag-tree          - Show DAG tree structure
 *   dag-gantt         - Show Gantt chart
 *   dag-critical      - Show critical path
 *   dag-html          - Generate HTML DAG view
 *   run-task <taskId> - Run a specific task
 *   run-dag           - Run all tasks in DAG order
 *   run-tasks <ids>   - Run specific tasks with dependencies
 */
object TaskRunner extends ZIOAppDefault:

  def run =
    for
      args <- getArgs
      _ <- args.headOption match
        case Some("list") =>
          ZIO.logInfo(TaskDAG.listTasks)

        case Some("info") =>
          args.lift(1) match
            case Some(taskId) =>
              TaskDAG.getTaskInfo(taskId) match
                case Some(info) => ZIO.logInfo(info)
                case None => ZIO.logError(s"Task not found: $taskId")
            case None =>
              ZIO.logError("Usage: info <taskId>")

        case Some("dag") =>
          ZIO.logInfo(DAGVisualizer.displayDAG)

        case Some("dag-tree") =>
          ZIO.logInfo(DAGVisualizer.displayDAGTree)

        case Some("dag-gantt") =>
          ZIO.logInfo(DAGVisualizer.displayGanttChart)

        case Some("dag-critical") =>
          ZIO.logInfo(DAGVisualizer.displayCriticalPath)

        case Some("dag-html") =>
          ZIO.succeed {
            val html = DAGVisualizer.displayDAGHTML
            val path = "dag-visualization.html"
            java.nio.file.Files.write(
              java.nio.file.Paths.get(path),
              html.getBytes(java.nio.charset.StandardCharsets.UTF_8)
            )
            println(s"✅ HTML DAG saved to: $path")
            println(s"Open in browser: file://${java.nio.file.Paths.get(path).toAbsolutePath}")
          }

        case Some("run-task") =>
          args.lift(1) match
            case Some(taskId) =>
              for
                result <- TaskDAG.runTask(taskId)
                  .catchAll(error =>
                    ZIO.logError(s"Error: ${error.getMessage}") *>
                    ZIO.fail(error)
                  )
                _ <- ZIO.logInfo(s"\nTask Result: ${result.toJson}")
              yield ()
            case None =>
              ZIO.logError("Usage: run-task <taskId>")

        case Some("run-dag") =>
          for
            results <- TaskDAG.runDAG
            _ <- ZIO.logInfo("\nResults:")
            _ <- ZIO.foreach(results) { result =>
              ZIO.logInfo(s"  ${result.taskId}: ${if result.success then "✅" else "❌"}")
            }
          yield ()

        case Some("run-tasks") =>
          val taskIds = args.drop(1).toList
          if taskIds.isEmpty then
            ZIO.logError("Usage: run-tasks <taskId1> <taskId2> ...")
          else
            for
              results <- TaskDAG.runTasks(taskIds)
              _ <- ZIO.logInfo("\nResults:")
              _ <- ZIO.foreach(results) { result =>
                ZIO.logInfo(s"  ${result.taskId}: ${if result.success then "✅" else "❌"}")
              }
            yield ()

        case _ =>
          ZIO.logInfo("""
            |Usage: sbt "orchestrator/runMain orchestrator.TaskRunner [command]"
            |
            |Commands (List & Info):
            |  list                    - List all available tasks
            |  info <taskId>           - Show task information
            |
            |Commands (DAG Visualization - Airflow Style):
            |  dag                     - Display DAG in table format
            |  dag-tree                - Display DAG tree structure
            |  dag-gantt               - Display Gantt/Timeline chart
            |  dag-critical            - Show critical path analysis
            |  dag-html                - Generate HTML visualization
            |
            |Commands (Task Execution):
            |  run-task <taskId>       - Run a specific task
            |  run-tasks <id1> <id2>   - Run specific tasks with dependencies
            |  run-dag                 - Run all tasks in order
            |
            |Examples:
            |  sbt "orchestrator/runMain orchestrator.TaskRunner list"
            |  sbt "orchestrator/runMain orchestrator.TaskRunner info publish_to_kafka"
            |  sbt "orchestrator/runMain orchestrator.TaskRunner dag"
            |  sbt "orchestrator/runMain orchestrator.TaskRunner dag-gantt"
            |  sbt "orchestrator/runMain orchestrator.TaskRunner dag-html"
            |  sbt "orchestrator/runMain orchestrator.TaskRunner run-task publish_to_kafka"
            |  sbt "orchestrator/runMain orchestrator.TaskRunner run-dag"
            |""".stripMargin)
    yield ()