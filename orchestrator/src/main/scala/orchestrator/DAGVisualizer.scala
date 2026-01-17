package orchestrator

import zio.*

/**
 * DAG Visualizer - Displays task DAG like Apache Airflow UI
 *
 * Shows dependency graph, execution flow, and task relationships
 * entirely in Scala/ZIO without external tools
 */
object DAGVisualizer:

  /**
   * Display Airflow-style DAG visualization
   */
  def displayDAG: String =
    val sb = StringBuilder()

    // Header
    sb.append("╔════════════════════════════════════════════════════════════════════════════╗\n")
    sb.append("║                     TASK DAG - Airflow Style                              ║\n")
    sb.append("║                  (Pure Scala/ZIO Implementation)                          ║\n")
    sb.append("╚════════════════════════════════════════════════════════════════════════════╝\n\n")

    // DAG Info
    sb.append("📊 DAG Information\n")
    sb.append("─" * 80 + "\n")
    sb.append(f"DAG ID:        pipeline_orchestrator\n")
    sb.append(f"Owner:         data-pipeline\n")
    sb.append(f"Tasks:         ${AllTasks.tasks.length}\n")
    sb.append(f"Schedule:      Hourly (0 * * * *)\n")
    sb.append(f"Tags:          data-pipeline, scala, zio\n\n")

    // Dependency visualization
    sb.append("📦 Task Dependency Graph\n")
    sb.append("─" * 80 + "\n\n")

    AllTasks.getTasksInOrder.foreach { task =>
      val icon = getTaskIcon(task.taskType)
      sb.append(s"$icon ${task.id}\n")
      sb.append(s"   ├─ Description: ${task.description}\n")
      sb.append(s"   ├─ Type: ${task.taskType}\n")
      sb.append(s"   ├─ Est. Duration: ${task.estimatedDuration}s\n")

      if task.dependsOn.nonEmpty then
        sb.append(s"   ├─ Depends on:\n")
        task.dependsOn.foreach { dep =>
          sb.append(s"   │  └─ $dep\n")
        }

      sb.append("   └─ Status: pending\n\n")
    }

    // Execution flow
    sb.append("🔄 Execution Flow\n")
    sb.append("─" * 80 + "\n\n")
    AllTasks.getTasksInOrder.zipWithIndex.foreach { (task, idx) =>
      val arrow = if idx < AllTasks.getTasksInOrder.length - 1 then "  │\n  ▼" else "  ◀─ START"
      sb.append(s"[${idx + 1}] ${task.id}\n$arrow\n")
    }

    sb.append("\n")

    // Task table
    sb.append("📋 Task Details\n")
    sb.append("─" * 80 + "\n")
    sb.append("%-25s %-20s %-15s %-10s\n".format("Task ID", "Type", "Dependencies", "Duration"))
    sb.append("─" * 80 + "\n")

    AllTasks.tasks.foreach { task =>
      val deps = if task.dependsOn.isEmpty then "None" else task.dependsOn.mkString(", ")
      sb.append("%-25s %-20s %-15s %-10s\n".format(
        task.id,
        task.taskType,
        if deps.length > 15 then deps.take(12) + "..." else deps,
        s"~${task.estimatedDuration}s"
      ))
    }

    sb.append("\n")

    // Summary
    val totalDuration = AllTasks.tasks.map(_.estimatedDuration).sum
    sb.append("📊 Summary Statistics\n")
    sb.append("─" * 80 + "\n")
    sb.append(f"Total Tasks:           ${AllTasks.tasks.length}\n")
    sb.append(f"Total Dependencies:    ${AllTasks.tasks.map(_.dependsOn.length).sum}\n")
    sb.append(f"Est. Total Duration:   ~${totalDuration}s (~${totalDuration / 60} min)\n")
    sb.append(f"Parallelization:       None (sequential execution)\n")
    sb.append(f"Fault Tolerance:       Fail on first error\n\n")

    sb.toString()

  /**
   * Display graphical DAG tree
   */
  def displayDAGTree: String =
    val sb = StringBuilder()

    sb.append("╔════════════════════════════════════════════════════════════════════════════╗\n")
    sb.append("║                         DAG TREE STRUCTURE                                ║\n")
    sb.append("╚════════════════════════════════════════════════════════════════════════════╝\n\n")

    def renderNode(t: orchestrator.Task, depth: Int, isLast: Boolean): Unit =
      val prefix = if isLast then "└── " else "├── "
      val icon = getTaskIcon(t.taskType)
      sb.append("  " * depth + prefix + icon + " " + t.id + "\n")

      if t.dependsOn.nonEmpty then
        t.dependsOn.zipWithIndex.foreach { (depId, idx) =>
          val childPrefix = if isLast then "    " else "│   "
          val childIsLast = idx == t.dependsOn.length - 1
          val childConnector = if childIsLast then "└── " else "├── "
          sb.append("  " * depth + childPrefix + childConnector + "← " + depId + "\n")
        }

    AllTasks.getTasksInOrder.foreach { task =>
      renderNode(task, 0, true)
    }

    sb.append("\n")
    sb.toString()

  /**
   * Display task critical path
   */
  def displayCriticalPath: String =
    val sb = StringBuilder()

    sb.append("╔════════════════════════════════════════════════════════════════════════════╗\n")
    sb.append("║                         CRITICAL PATH ANALYSIS                            ║\n")
    sb.append("╚════════════════════════════════════════════════════════════════════════════╝\n\n")

    // Calculate critical path
    def calculatePath(taskId: String, visited: Set[String] = Set()): (List[String], Int) =
      AllTasks.getTask(taskId) match
        case None => (List(), 0)
        case Some(task) =>
          if visited.contains(taskId) then (List(), 0)
          else
            val pathsFromDeps = task.dependsOn.map { dep =>
              calculatePath(dep, visited + taskId)
            }
            val maxDepPath = pathsFromDeps.maxByOption(_._2).getOrElse((List(), 0))
            (taskId :: maxDepPath._1, task.estimatedDuration + maxDepPath._2)

    val (path, totalDuration) = AllTasks.getTasksInOrder.lastOption
      .map(task => calculatePath(task.id))
      .getOrElse((List(), 0))

    sb.append("📍 Critical Path (Longest Path):\n")
    sb.append("─" * 80 + "\n")

    path.reverse.zipWithIndex.foreach { (taskId, idx) =>
      val arrow = if idx < path.length - 1 then "  ↓" else "  ◀─ END"
      AllTasks.getTask(taskId).foreach { task =>
        sb.append(f"$idx. $taskId (${task.estimatedDuration}s)\n$arrow\n")
      }
    }

    sb.append("\n")
    sb.append(f"Total Critical Path Duration: ${totalDuration}s (~${totalDuration / 60} min)\n")
    sb.append(f"Parallelization Possible:    None (all tasks are sequential)\n")
    sb.append(f"Bottleneck:                  All stages (equal criticality)\n\n")

    sb.toString()

  /**
   * Display Gantt chart view
   */
  def displayGanttChart: String =
    val sb = StringBuilder()

    sb.append("╔════════════════════════════════════════════════════════════════════════════╗\n")
    sb.append("║                     TASK TIMELINE / GANTT CHART                           ║\n")
    sb.append("╚════════════════════════════════════════════════════════════════════════════╝\n\n")

    sb.append("Time(s)  0        10        20        30        40        50        60\n")
    sb.append("         |────────|────────|────────|────────|────────|────────|────────|\n")

    var currentTime = 0
    AllTasks.getTasksInOrder.foreach { task =>
      val taskBar = "█" * (task.estimatedDuration / 2)
      val spacing = " " * (currentTime / 2)
      sb.append(f"$spacing$taskBar ${task.id}\n")
      currentTime += task.estimatedDuration
    }

    sb.append("         |────────|────────|────────|────────|────────|────────|────────|\n")
    sb.append(f"Total Duration: ~${currentTime}s (~${currentTime / 60} min)\n\n")

    sb.toString()

  /**
   * Display web UI HTML for DAG
   */
  def displayDAGHTML: String =
    s"""<!DOCTYPE html>
      |<html>
      |<head>
      |    <title>Task DAG Visualization</title>
      |    <style>
      |        body { font-family: Arial; margin: 20px; background: #f5f5f5; }
      |        .container { max-width: 1200px; margin: 0 auto; }
      |        .dag { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
      |        .task { display: inline-block; margin: 10px; padding: 15px; background: #667eea; color: white; border-radius: 6px; min-width: 120px; text-align: center; }
      |        .task.producer { background: #4CAF50; }
      |        .task.processor { background: #2196F3; }
      |        .task.sensor { background: #FF9800; }
      |        .task.analyzer { background: #9C27B0; }
      |        .arrow { display: inline-block; margin: 0 10px; color: #667eea; font-weight: bold; }
      |        h1 { color: #333; }
      |        table { width: 100%; border-collapse: collapse; margin-top: 20px; }
      |        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
      |        th { background-color: #667eea; color: white; }
      |        tr:hover { background-color: #f5f5f5; }
      |    </style>
      |</head>
      |<body>
      |    <div class="container">
      |        <h1>📊 Task DAG Visualization</h1>
      |        <div class="dag">
      |            <h2>Execution Flow</h2>
      |            <div style="overflow-x: auto;">
      |${AllTasks.getTasksInOrder.zipWithIndex.map { (task, idx) =>
          val cssClass = task.taskType.toLowerCase
          val arrow = if idx < AllTasks.getTasksInOrder.length - 1 then "<span class='arrow'>→</span>" else ""
          s"                <span class='task $cssClass'>${task.id}</span>$arrow"
        }.mkString("\n")}
      |            </div>
      |        </div>
      |
      |        <h2>Task Details</h2>
      |        <table>
      |            <tr>
      |                <th>Task ID</th>
      |                <th>Description</th>
      |                <th>Type</th>
      |                <th>Dependencies</th>
      |                <th>Duration</th>
      |            </tr>
      |${AllTasks.tasks.map { task =>
          s"""            <tr>
             |                <td>${task.id}</td>
             |                <td>${task.description}</td>
             |                <td>${task.taskType}</td>
             |                <td>${if task.dependsOn.isEmpty then "None" else task.dependsOn.mkString(", ")}</td>
             |                <td>~${task.estimatedDuration}s</td>
             |            </tr>""".stripMargin
        }.mkString("\n")}
      |        </table>
      |    </div>
      |</body>
      |</html>""".stripMargin

  private def getTaskIcon(taskType: String): String =
    taskType match
      case "Producer" => "📤"
      case "Processor" => "🔨"
      case "Sensor" => "⏳"
      case "Analyzer" => "🚀"
      case _ => "📦"
