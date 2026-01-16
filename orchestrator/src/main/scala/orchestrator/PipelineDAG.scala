package orchestrator

import zio.*

/**
 * Pipeline DAG (Directed Acyclic Graph) representation
 *
 * Explicitly models the pipeline stages and their dependencies:
 *
 * ┌─────────────────────────────────────────────────┐
 * │ PublishToKafka                                  │
 * │ (Generate & publish 100 events)                 │
 * └──────────────┬──────────────────────────────────┘
 *                │
 *                ▼
 * ┌─────────────────────────────────────────────────┐
 * │ SubmitFlinkJob                                  │
 * │ (Build & submit Flink streaming job)            │
 * └──────────────┬──────────────────────────────────┘
 *                │
 *                ▼
 * ┌─────────────────────────────────────────────────┐
 * │ WaitForParquetOutput                            │
 * │ (Poll data lake for Parquet files)              │
 * └──────────────┬──────────────────────────────────┘
 *                │
 *                ▼
 * ┌─────────────────────────────────────────────────┐
 * │ RunSparkETL                                     │
 * │ (Build & run Spark batch analytics)             │
 * └─────────────────────────────────────────────────┘
 */

sealed trait PipelineStage:
  def name: String
  def description: String
  def dependencies: List[PipelineStage]

case object PublishToKafka extends PipelineStage:
  override def name = "PublishToKafka"
  override def description = "Generate & publish 100 events to Kafka"
  override def dependencies = List()

case object SubmitFlinkJob extends PipelineStage:
  override def name = "SubmitFlinkJob"
  override def description = "Build & submit Flink streaming job"
  override def dependencies = List(PublishToKafka)

case object WaitForParquetOutput extends PipelineStage:
  override def name = "WaitForParquetOutput"
  override def description = "Poll data lake for Parquet files"
  override def dependencies = List(SubmitFlinkJob)

case object RunSparkETL extends PipelineStage:
  override def name = "RunSparkETL"
  override def description = "Build & run Spark batch analytics"
  override def dependencies = List(WaitForParquetOutput)

/**
 * Pipeline DAG - encapsulates all stages and their execution order
 */
object PipelineDAG:

  /**
   * All stages in topological order (execution order)
   */
  val allStages: List[PipelineStage] = List(
    PublishToKafka,
    SubmitFlinkJob,
    WaitForParquetOutput,
    RunSparkETL
  )

  /**
   * Get the execution order (topological sort)
   */
  def executionOrder: List[PipelineStage] = allStages

  /**
   * Get stages that can run in parallel (none in this case - all sequential)
   */
  def parallelStages: Map[Int, List[PipelineStage]] =
    allStages.zipWithIndex.groupBy(_._2).view.mapValues(_.map(_._1)).toMap

  /**
   * Check if all stages are satisfied (can be executed)
   */
  def canExecute(stage: PipelineStage, completed: Set[PipelineStage]): Boolean =
    stage.dependencies.forall(completed.contains)

  /**
   * ASCII DAG visualization
   */
  def visualizeDAG: String =
    """
      |╔═══════════════════════════════════════════════════════════════╗
      |║            PIPELINE DAG (Directed Acyclic Graph)             ║
      |╚═══════════════════════════════════════════════════════════════╝
      |
      |    Stage 1                    Stage 2                Stage 3
      |    ───────                    ───────                ───────
      |
      |┌──────────────────┐
      |│ PublishToKafka   │         Generate & publish 100 test events
      |│   (📤 Publish)   │         to Kafka topic "people-events"
      |└────────┬─────────┘
      |         │
      |         │ (PersonEvent messages)
      |         │
      |         ▼
      |┌──────────────────┐
      |│ SubmitFlinkJob   │         Build Flink job JAR
      |│   (🔨 Build)     │         Submit to Flink cluster
      |│   (📮 Submit)    │         Extract Job ID
      |└────────┬─────────┘
      |         │
      |         │ (Job submitted, processing started)
      |         │
      |         ▼
      |┌──────────────────┐
      |│ WaitForOutput    │         Poll data/streaming/people/
      |│   (⏳ Wait)      │         for Parquet files
      |│   (timeout: 120s)│         Check every 5 seconds
      |└────────┬─────────┘
      |         │
      |         │ (Parquet files found)
      |         │
      |         ▼
      |┌──────────────────┐
      |│ RunSparkETL      │         Build Spark ETL JAR
      |│   (🚀 Analyze)   │         Submit to Spark
      |│                  │         Run batch analytics
      |└──────────────────┘
      |
      |
      |═══════════════════════════════════════════════════════════════
      |
      |Execution Model:    SEQUENTIAL (each stage depends on previous)
      |Parallelization:    NONE (async Flink processing happens in parallel)
      |Fault Tolerance:    STOP on first failure (no retries)
      |Total Stages:       4
      |Total Dependencies: 3
      |
      |═══════════════════════════════════════════════════════════════
      |""".stripMargin

  /**
   * Detailed dependency graph
   */
  def dependencyGraph: String =
    val sb = StringBuilder()
    sb.append("┌─ Dependency Graph ─────────────────────────────────────┐\n")
    allStages.foreach { stage =>
      sb.append(s"│ ${stage.name}\n")
      if stage.dependencies.isEmpty then
        sb.append(s"│   └─ (no dependencies)\n")
      else
        stage.dependencies.zipWithIndex.foreach { (dep, idx) =>
          val isLast = idx == stage.dependencies.length - 1
          val prefix = if isLast then "└─" else "├─"
          sb.append(s"│   $prefix ${dep.name}\n")
        }
    }
    sb.append("└─────────────────────────────────────────────────────────┘\n")
    sb.toString()

  /**
   * Stage execution timeline
   */
  def timeline: String =
    val sb = StringBuilder()
    sb.append("Time ──────────────────────────────────────────────────────>\n")
    sb.append("  │\n")
    allStages.zipWithIndex.foreach { (stage, idx) =>
      sb.append(s"  ├─ T${idx + 1}: ${stage.name}\n")
      sb.append(s"  │       ${stage.description}\n")
    }
    sb.append("  │\n")
    sb.append("  └─ Done!\n")
    sb.toString()

  /**
   * Execution trace with estimated times
   */
  def executionTrace: String =
    """
      |╔═══════════════════════════════════════════════════════════╗
      |║           PIPELINE EXECUTION TRACE (Estimated)           ║
      |╚═══════════════════════════════════════════════════════════╝
      |
      |T=0s   📤 PublishToKafka
      |       ├─ Compile preprocessing module          ~5s
      |       ├─ Generate 100 test events              ~1s
      |       └─ Publish to Kafka                      ~2s
      |
      |T=8s   🔨 SubmitFlinkJob
      |       ├─ Compile flink-streaming module        ~10s
      |       ├─ Build assembly JAR                    ~20s
      |       ├─ Submit to Flink                       ~2s
      |       └─ Extract Job ID                        ~1s
      |
      |T=41s  ⏳ WaitForParquetOutput
      |       ├─ Flink job starts processing           ~5s
      |       ├─ First event reaches sink              ~10s
      |       ├─ Poll for Parquet files                ~1-120s
      |       └─ Files found                           ~15s (avg)
      |
      |T=71s  🚀 RunSparkETL
      |       ├─ Compile etl module                    ~8s
      |       ├─ Build assembly JAR                    ~15s
      |       ├─ Submit to Spark                       ~2s
      |       └─ Process data & aggregate              ~30s
      |
      |T=126s ✅ COMPLETE
      |
      |Total Pipeline Time: ~126 seconds (2 minutes)
      |
      |Note: Times are estimates. Actual times depend on:
      |  • System performance
      |  • Kafka/Flink/Spark configuration
      |  • Data size and complexity
      |  • Network latency
      |
      |""".stripMargin

  /**
   * Critical path analysis
   */
  def criticalPath: String =
    val path = executionOrder.map(_.name).mkString(" → ")
    s"""
      |Critical Path (longest dependency chain):
      |$path
      |
      |All paths are critical (sequential execution).
      |No parallel execution possible.
      |Bottleneck: Data processing time at each stage.
      |""".stripMargin

/**
 * ZIO effects for DAG visualization and execution
 */
object PipelineDAGEffects:

  /**
   * Display the DAG
   */
  def visualize: ZIO[Any, Nothing, Unit] =
    ZIO.succeed {
      println(PipelineDAG.visualizeDAG)
    }

  /**
   * Display dependency graph
   */
  def showDependencies: ZIO[Any, Nothing, Unit] =
    ZIO.succeed {
      println(PipelineDAG.dependencyGraph)
    }

  /**
   * Display timeline
   */
  def showTimeline: ZIO[Any, Nothing, Unit] =
    ZIO.succeed {
      println(PipelineDAG.timeline)
    }

  /**
   * Display execution trace with timing
   */
  def showExecutionTrace: ZIO[Any, Nothing, Unit] =
    ZIO.succeed {
      println(PipelineDAG.executionTrace)
    }

  /**
   * Display critical path
   */
  def showCriticalPath: ZIO[Any, Nothing, Unit] =
    ZIO.succeed {
      println(PipelineDAG.criticalPath)
    }

  /**
   * Dry-run: show what would execute without actually running it
   */
  def dryRun: ZIO[Any, Nothing, Unit] =
    for
      _ <- visualize
      _ <- showDependencies
      _ <- showTimeline
      _ <- ZIO.logInfo("Would execute pipeline with these stages:")
      _ <- ZIO.foreach(PipelineDAG.allStages) { stage =>
        ZIO.logInfo(s"  → ${stage.name}: ${stage.description}")
      }
    yield ()

  /**
   * Show stage details
   */
  def showStageDetails: ZIO[Any, Nothing, Unit] =
    ZIO.succeed {
      println("\n╔═ Stage Details ════════════════════════════════════════╗")
      PipelineDAG.allStages.zipWithIndex.foreach { (stage, idx) =>
        println(s"║ Stage ${idx + 1}: ${stage.name}")
        println(s"║ Description: ${stage.description}")
        println(s"║ Dependencies: ${if stage.dependencies.isEmpty then "None" else stage.dependencies.map(_.name).mkString(", ")}")
        if idx < PipelineDAG.allStages.length - 1 then println("╟─" + "─" * 50)
      }
      println("╚" + "═" * 55 + "╝\n")
    }

/**
 * Entry point for DAG visualization
 */
object PipelineDAGMain extends ZIOAppDefault:
  def run =
    for
      args <- getArgs
      _ <- args.headOption match
        case Some("dag") => PipelineDAGEffects.visualize
        case Some("dependencies") => PipelineDAGEffects.showDependencies
        case Some("timeline") => PipelineDAGEffects.showTimeline
        case Some("trace") => PipelineDAGEffects.showExecutionTrace
        case Some("critical") => PipelineDAGEffects.showCriticalPath
        case Some("stages") => PipelineDAGEffects.showStageDetails
        case Some("dry-run") => PipelineDAGEffects.dryRun
        case Some("all") =>
          for
            _ <- PipelineDAGEffects.visualize
            _ <- PipelineDAGEffects.showDependencies
            _ <- PipelineDAGEffects.showTimeline
            _ <- PipelineDAGEffects.showStageDetails
            _ <- PipelineDAGEffects.showExecutionTrace
            _ <- PipelineDAGEffects.showCriticalPath
          yield ()
        case _ =>
          ZIO.logInfo("""
            |Usage: sbt "orchestrator/runMain orchestrator.PipelineDAGMain [command]"
            |
            |Commands:
            |  dag           - Show ASCII DAG visualization
            |  dependencies  - Show dependency graph
            |  timeline      - Show execution timeline
            |  trace         - Show execution trace with timing
            |  critical      - Show critical path analysis
            |  stages        - Show detailed stage information
            |  dry-run       - Show what would execute (dry run)
            |  all           - Show all visualizations
            |
            |Examples:
            |  sbt "orchestrator/runMain orchestrator.PipelineDAGMain dag"
            |  sbt "orchestrator/runMain orchestrator.PipelineDAGMain timeline"
            |  sbt "orchestrator/runMain orchestrator.PipelineDAGMain all"
            |""".stripMargin)
    yield ()
