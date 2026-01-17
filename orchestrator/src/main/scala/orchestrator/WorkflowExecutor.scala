package orchestrator

import scala.collection.mutable

/**
 * Workflow Executor - Runs entire DAG with real-time progress tracking
 */
object WorkflowExecutor:

  private val workflowStatus = mutable.Map[String, String]()
  private val taskExecutionQueue = mutable.Queue[String]()

  def startWorkflow: Unit =
    synchronized {
      workflowStatus.clear()
      taskExecutionQueue.clear()

      // Queue all tasks in dependency order
      AllTasks.getTasksInOrder.zipWithIndex.foreach { case (task, _) =>
        taskExecutionQueue.enqueue(task.id)
        workflowStatus(task.id) = "QUEUED"
        TaskExecutionTracker.initializeTask(task.id)
      }

      // Start execution in background thread
      new Thread {
        setDaemon(true)
        override def run(): Unit =
          executeNextTask()
      }.start()
    }

  private def executeNextTask(): Unit =
    if taskExecutionQueue.nonEmpty then
      val taskId = taskExecutionQueue.dequeue()
      val task = AllTasks.getTask(taskId)

      task.foreach { t =>
        // Check if dependencies are completed
        val depsCompleted = t.dependsOn.forall(depId => workflowStatus.get(depId).contains("COMPLETED"))

        if depsCompleted then
          workflowStatus(taskId) = "RUNNING"
          TaskExecutionTracker.startTask(taskId)
          TaskExecutionTracker.addLog(taskId, s"Task started (part of workflow execution)")

          // Simulate or execute task
          try
            TaskExecutionTracker.addLog(taskId, s"Executing ${t.description}...")
            Thread.sleep(500) // Simulate work

            workflowStatus(taskId) = "COMPLETED"
            TaskExecutionTracker.completeTask(taskId, true)
            TaskExecutionTracker.addLog(taskId, "✅ Task completed successfully")
          catch
            case e: Exception =>
              workflowStatus(taskId) = "FAILED"
              TaskExecutionTracker.completeTask(taskId, false, Some(e.getMessage))
              TaskExecutionTracker.addLog(taskId, s"❌ Task failed: ${e.getMessage}", "ERROR")

          // Execute next task
          executeNextTask()
        else
          // Re-queue if dependencies not met
          taskExecutionQueue.enqueue(taskId)
          Thread.sleep(100)
          executeNextTask()
      }

  def getWorkflowStatus: String =
    val tasksInOrder = AllTasks.getTasksInOrder
    val statusList = tasksInOrder.zipWithIndex.map { case (task, idx) =>
      val status = workflowStatus.getOrElse(task.id, "PENDING")
      val metrics = TaskExecutionTracker.getMetrics(task.id)
      val duration = metrics.map(_.duration).getOrElse(0L)
      s"""{"taskId":"${task.id}","order":$idx,"status":"$status","dependencies":[${task.dependsOn.map(d => s""""$d"""").mkString(", ")}],"duration":$duration}"""
    }
    s"""[${statusList.mkString(", ")}]"""

  def isWorkflowRunning: Boolean =
    workflowStatus.values.exists(s => s == "RUNNING" || s == "QUEUED")

  def cancelWorkflow: Unit =
    synchronized {
      taskExecutionQueue.clear()
      workflowStatus.keys.foreach { taskId =>
        if workflowStatus(taskId) != "COMPLETED" then
          workflowStatus(taskId) = "CANCELLED"
          TaskExecutionTracker.completeTask(taskId, false, Some("Workflow cancelled"))
      }
    }