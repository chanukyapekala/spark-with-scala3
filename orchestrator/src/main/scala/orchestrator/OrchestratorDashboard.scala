package orchestrator

import zio.*

/**
 * Interactive Orchestrator Dashboard
 *
 * Usage:
 *   sbt "orchestrator/runMain orchestrator.OrchestratorDashboard"
 *
 * Then open: http://localhost:9090
 *
 * Features:
 * - Interactive task cards with click-to-see-details
 * - Real-time execution tracking
 * - Task execution logs and metrics
 * - Records processed, timing, error tracking
 * - Status updates (PENDING, RUNNING, COMPLETED, FAILED)
 */
object OrchestratorDashboard extends ZIOAppDefault:

  def run =
    for
      _ <- Console.printLine("")
      _ <- Console.printLine("╔════════════════════════════════════════════════════════════════╗")
      _ <- Console.printLine("║      🎯 Pipeline Orchestrator Dashboard - Interactive        ║")
      _ <- Console.printLine("║                                                              ║")
      _ <- Console.printLine("║  🌐 Open in browser: http://localhost:9090                  ║")
      _ <- Console.printLine("║                                                              ║")
      _ <- Console.printLine("║  Features:                                                   ║")
      _ <- Console.printLine("║  ✓ Interactive task cards                                    ║")
      _ <- Console.printLine("║  ✓ Real-time execution tracking                              ║")
      _ <- Console.printLine("║  ✓ Execution logs & metrics                                  ║")
      _ <- Console.printLine("║  ✓ Records, timing, error tracking                           ║")
      _ <- Console.printLine("║  ✓ Live status updates                                       ║")
      _ <- Console.printLine("║                                                              ║")
      _ <- Console.printLine("║  Click any task to view details and run it                   ║")
      _ <- Console.printLine("║                                                              ║")
      _ <- Console.printLine("╚════════════════════════════════════════════════════════════════╝")
      _ <- Console.printLine("")

      // Initialize all tasks in the tracker (synchronous)
      _ <- ZIO.succeed {
        AllTasks.tasks.foreach { task =>
          TaskExecutionTracker.initializeTask(task.id)
        }
      }

      // Start the interactive dashboard
      _ <- InteractiveOrchestrator.start

      // Keep the server running
      _ <- ZIO.never
    yield ()