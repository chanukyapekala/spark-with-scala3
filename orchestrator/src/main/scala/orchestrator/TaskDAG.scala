package orchestrator

import zio.*
import scala.collection.mutable

/**
 * Task DAG - Directed Acyclic Graph of tasks
 *
 * Manages task execution with dependency resolution, similar to Apache Airflow
 */
object TaskDAG:

  /**
   * Execute a single task by ID
   */
  def runTask(taskId: String): ZIO[Any, Throwable, TaskResult] =
    for
      task <- ZIO.fromOption(AllTasks.getTask(taskId))
        .mapError(_ => Exception(s"Task not found: $taskId"))
      _ <- ZIO.logInfo(s"▶️  Running task: $taskId")
      result <- task.run
      _ <- ZIO.logInfo(s"⏱️  Task duration: ${result.duration}ms")
    yield result

  /**
   * Execute entire DAG sequentially based on dependencies
   */
  def runDAG: ZIO[Any, Throwable, List[TaskResult]] =
    for
      _ <- ZIO.logInfo("=" * 80)
      _ <- ZIO.logInfo("🎯 Starting Task DAG Execution")
      _ <- ZIO.logInfo("=" * 80)
      results <- ZIO.foreach(AllTasks.getTasksInOrder) { task =>
        for
          _ <- ZIO.logInfo("")
          _ <- ZIO.logInfo(s"📦 Task: ${task.id}")
          _ <- ZIO.logInfo(s"   Description: ${task.description}")
          _ <- ZIO.logInfo(s"   Type: ${task.taskType}")
          _ <- ZIO.logInfo(s"   Dependencies: ${if task.dependsOn.isEmpty then "None" else task.dependsOn.mkString(", ")}")
          result <- runTask(task.id).catchAll { error =>
            ZIO.succeed(TaskResult(
              taskId = task.id,
              success = false,
              output = "",
              error = Some(error.getMessage),
              duration = 0
            ))
          }
        yield result
      }
      _ <- ZIO.logInfo("")
      _ <- ZIO.logInfo("=" * 80)
      _ <- ZIO.logInfo("📊 DAG Execution Summary")
      _ <- ZIO.logInfo("=" * 80)
      _ <- displaySummary(results)
    yield results

  /**
   * Execute specific tasks with dependency validation
   */
  def runTasks(taskIds: List[String]): ZIO[Any, Throwable, List[TaskResult]] =
    for
      // Validate all tasks exist
      tasks <- ZIO.foreach(taskIds) { id =>
        ZIO.fromOption(AllTasks.getTask(id))
          .mapError(_ => Exception(s"Task not found: $id"))
      }

      // Resolve dependencies
      allRequired <- resolveDependencies(taskIds)

      _ <- ZIO.logInfo(s"📋 Requested tasks: ${taskIds.mkString(", ")}")
      _ <- ZIO.logInfo(s"📦 Including dependencies: ${(allRequired -- taskIds).mkString(", ")}")

      // Execute in order
      results <- ZIO.foreach(AllTasks.getTasksInOrder) { task =>
        if allRequired.contains(task.id) then
          for
            _ <- ZIO.logInfo(s"▶️  Running: ${task.id}")
            result <- task.run
          yield Some(result)
        else
          ZIO.succeed(None)
      }
    yield results.flatten

  /**
   * Resolve all dependencies for given tasks (transitive closure)
   */
  private def resolveDependencies(taskIds: List[String]): ZIO[Any, Throwable, Set[String]] =
    ZIO.succeed {
      val visited = mutable.Set[String]()
      val toVisit = mutable.Queue[String](taskIds*)

      while toVisit.nonEmpty do
        val current = toVisit.dequeue()
        if !visited.contains(current) then
          visited.add(current)
          AllTasks.getTask(current).foreach { task =>
            task.dependsOn.foreach(dep => toVisit.enqueue(dep))
          }

      visited.toSet
    }

  /**
   * Visualize DAG as text
   */
  def visualizeDAG: String =
    val sb = StringBuilder()
    sb.append("╔════════════════════════════════════════════════════════════════╗\n")
    sb.append("║                    Task DAG (Dependency Graph)               ║\n")
    sb.append("╚════════════════════════════════════════════════════════════════╝\n\n")

    AllTasks.getTasksInOrder.foreach { task =>
      val icon = task.taskType match
        case "Producer" => "📤"
        case "Processor" => "🔨"
        case "Sensor" => "⏳"
        case "Analyzer" => "🚀"
        case _ => "📦"

      sb.append(s"$icon ${task.id}\n")
      sb.append(s"   Description: ${task.description}\n")
      sb.append(s"   Type: ${task.taskType}\n")
      sb.append(s"   Est. Duration: ${task.estimatedDuration}s\n")

      if task.dependsOn.nonEmpty then
        sb.append(s"   Depends on: ${task.dependsOn.mkString(", ")}\n")
        task.dependsOn.zipWithIndex.foreach { (dep, idx) =>
          val connector = if idx == task.dependsOn.length - 1 then "└─" else "├─"
          sb.append(s"   $connector ← $dep\n")
        }

      sb.append("\n")
    }

    // Add dependency arrows
    sb.append("Execution Order:\n")
    AllTasks.getTasksInOrder.zipWithIndex.foreach { (task, idx) =>
      sb.append(s"  ${idx + 1}. ${task.id}")
      if idx < AllTasks.getTasksInOrder.length - 1 then sb.append(" ↓\n") else sb.append("\n")
    }

    sb.toString()

  /**
   * Display execution summary
   */
  private def displaySummary(results: List[TaskResult]): ZIO[Any, Nothing, Unit] =
    val successful = results.count(_.success)
    val failed = results.count(!_.success)
    val totalDuration = results.map(_.duration).sum

    ZIO.logInfo(s"✅ Successful: $successful/${results.length}") *>
    ZIO.logInfo(s"❌ Failed: $failed/${results.length}") *>
    ZIO.logInfo(s"⏱️  Total Duration: ${totalDuration}ms (~${totalDuration / 1000}s)") *>
    (if failed > 0 then
      ZIO.logError("\nFailed Tasks:") *>
      ZIO.foreach(results.filter(!_.success)) { result =>
        ZIO.logError(s"  • ${result.taskId}: ${result.error.getOrElse("Unknown error")}")
      }.unit
    else
      ZIO.logInfo("\n🎉 All tasks completed successfully!"))

  /**
   * Get task info
   */
  def getTaskInfo(taskId: String): Option[String] =
    AllTasks.getTask(taskId).map { task =>
      s"""Task: ${task.id}
Description: ${task.description}
Type: ${task.taskType}
Dependencies: ${if task.dependsOn.isEmpty then "None" else task.dependsOn.mkString(", ")}
Est. Duration: ${task.estimatedDuration}s"""
    }

  /**
   * List all tasks
   */
  def listTasks: String =
    val sb = StringBuilder()
    sb.append("Available Tasks:\n\n")
    AllTasks.tasks.foreach { task =>
      sb.append(s"${task.id}\n")
      sb.append(s"  ${task.description}\n")
      sb.append(s"  Type: ${task.taskType}\n\n")
    }
    sb.toString()