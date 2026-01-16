# Pipeline DAG (Directed Acyclic Graph)

Complete visualization and analysis of the pipeline execution graph using ZIO and Scala 3.

## Quick Start

### View the DAG
```bash
sbt "orchestrator/runMain orchestrator.PipelineDAGMain dag"
```

### View All Visualizations
```bash
sbt "orchestrator/runMain orchestrator.PipelineDAGMain all"
```

## Available Commands

| Command | Description | Output |
|---------|-------------|--------|
| `dag` | ASCII DAG visualization | Pipeline stages, dependencies, execution model |
| `dependencies` | Dependency graph | Each stage and its dependencies |
| `timeline` | Execution timeline | Chronological view of stages |
| `trace` | Execution trace with timing | Estimated time breakdown per stage |
| `critical` | Critical path analysis | Longest dependency chain & bottlenecks |
| `stages` | Stage details | Complete information about each stage |
| `dry-run` | Dry run (preview) | Show what would execute without running |
| `all` | All visualizations | Complete analysis report |

## Architecture

### Pipeline Stages (Scala 3 sealed trait)
```scala
sealed trait PipelineStage:
  def name: String
  def description: String
  def dependencies: List[PipelineStage]

case object PublishToKafka      extends PipelineStage
case object SubmitFlinkJob      extends PipelineStage
case object WaitForParquetOutput extends PipelineStage
case object RunSparkETL         extends PipelineStage
```

### DAG Structure
```
PublishToKafka
      ↓
SubmitFlinkJob
      ↓
WaitForParquetOutput
      ↓
RunSparkETL
```

**Type**: Sequential (linear dependency chain)
**Parallelization**: None (all stages must run in order)
**Stages**: 4
**Dependencies**: 3

## Execution Flow

### Stage 1: PublishToKafka
```
Purpose:    Generate and publish 100 test events to Kafka
Input:      None (root stage)
Output:     100 PersonEvent messages in kafka:9093/people-events
Time:       ~8 seconds
  ├─ Compile preprocessing module    ~5s
  ├─ Generate 100 test events        ~1s
  └─ Publish to Kafka               ~2s
```

### Stage 2: SubmitFlinkJob
```
Purpose:    Build and submit Flink streaming job
Input:      Kafka topic with events
Output:     Job submitted to Flink cluster (ID extracted)
Time:       ~33 seconds
  ├─ Compile flink-streaming module  ~10s
  ├─ Build assembly JAR             ~20s
  ├─ Submit to Flink                ~2s
  └─ Extract Job ID                 ~1s
```

### Stage 3: WaitForParquetOutput
```
Purpose:    Poll data lake for Parquet file output
Input:      Flink job processing events
Output:     Parquet files found in data/streaming/people/
Time:       ~30 seconds (average)
  ├─ Flink job starts processing     ~5s
  ├─ First event reaches sink        ~10s
  ├─ Poll for Parquet files          ~1-120s
  └─ Files found                     ~15s (avg)
```

### Stage 4: RunSparkETL
```
Purpose:    Build and run Spark batch analytics
Input:      Parquet files from data lake
Output:     Aggregated results in data/output/
Time:       ~55 seconds
  ├─ Compile etl module              ~8s
  ├─ Build assembly JAR             ~15s
  ├─ Submit to Spark                ~2s
  └─ Process data & aggregate       ~30s
```

## Example Outputs

### DAG Visualization
```
╔═══════════════════════════════════════════════════════════════╗
║            PIPELINE DAG (Directed Acyclic Graph)             ║
╚═══════════════════════════════════════════════════════════════╝

┌──────────────────┐
│ PublishToKafka   │         Generate & publish 100 test events
│   (📤 Publish)   │         to Kafka topic "people-events"
└────────┬─────────┘
         │ (PersonEvent messages)
         ▼
┌──────────────────┐
│ SubmitFlinkJob   │         Build Flink job JAR
│   (🔨 Build)     │         Submit to Flink cluster
│   (📮 Submit)    │         Extract Job ID
└────────┬─────────┘
         │ (Job submitted, processing started)
         ▼
┌──────────────────┐
│ WaitForOutput    │         Poll data/streaming/people/
│   (⏳ Wait)      │         for Parquet files
│   (timeout: 120s)│         Check every 5 seconds
└────────┬─────────┘
         │ (Parquet files found)
         ▼
┌──────────────────┐
│ RunSparkETL      │         Build Spark ETL JAR
│   (🚀 Analyze)   │         Submit to Spark
│                  │         Run batch analytics
└──────────────────┘

Execution Model:    SEQUENTIAL
Total Stages:       4
Total Dependencies: 3
```

### Execution Trace
```
T=0s   📤 PublishToKafka
       ├─ Compile preprocessing module          ~5s
       ├─ Generate 100 test events              ~1s
       └─ Publish to Kafka                      ~2s

T=8s   🔨 SubmitFlinkJob
       ├─ Compile flink-streaming module        ~10s
       ├─ Build assembly JAR                    ~20s
       ├─ Submit to Flink                       ~2s
       └─ Extract Job ID                        ~1s

T=41s  ⏳ WaitForParquetOutput
       ├─ Flink job starts processing           ~5s
       ├─ First event reaches sink              ~10s
       ├─ Poll for Parquet files                ~1-120s
       └─ Files found                           ~15s (avg)

T=71s  🚀 RunSparkETL
       ├─ Compile etl module                    ~8s
       ├─ Build assembly JAR                    ~15s
       ├─ Submit to Spark                       ~2s
       └─ Process data & aggregate              ~30s

T=126s ✅ COMPLETE

Total Pipeline Time: ~126 seconds (2 minutes)
```

### Critical Path
```
Critical Path (longest dependency chain):
PublishToKafka → SubmitFlinkJob → WaitForParquetOutput → RunSparkETL

All paths are critical (sequential execution).
No parallel execution possible.
Bottleneck: Data processing time at each stage.
```

## Dependency Graph

```
PublishToKafka
   └─ (no dependencies)

SubmitFlinkJob
   └─ PublishToKafka

WaitForParquetOutput
   └─ SubmitFlinkJob

RunSparkETL
   └─ WaitForParquetOutput
```

## Features

### Type-Safe DAG
- Sealed trait for pipeline stages
- Case objects for each stage
- Compile-time guarantees

### Visualization
- ASCII art DAG diagram
- Dependency graph
- Execution timeline
- Critical path analysis

### Analysis
- Stage descriptions
- Execution order verification
- Dependency satisfaction checking
- Parallel execution detection (none in this case)

### ZIO Integration
All visualizations are pure ZIO effects:
```scala
def visualize: ZIO[Any, Nothing, Unit]
def showDependencies: ZIO[Any, Nothing, Unit]
def showTimeline: ZIO[Any, Nothing, Unit]
def showExecutionTrace: ZIO[Any, Nothing, Unit]
def showCriticalPath: ZIO[Any, Nothing, Unit]
```

## Use Cases

### Planning
```bash
# Before running pipeline, see what will happen
sbt "orchestrator/runMain orchestrator.PipelineDAGMain all"
```

### Documentation
```bash
# Generate pipeline documentation
sbt "orchestrator/runMain orchestrator.PipelineDAGMain dag"
```

### Dry Run
```bash
# Preview execution without actually running
sbt "orchestrator/runMain orchestrator.PipelineDAGMain dry-run"
```

### Performance Analysis
```bash
# Understand bottlenecks and timing
sbt "orchestrator/runMain orchestrator.PipelineDAGMain trace"
```

### Critical Path Analysis
```bash
# Find what affects total execution time
sbt "orchestrator/runMain orchestrator.PipelineDAGMain critical"
```

## Future Enhancements

- [ ] Parallel stage detection & visualization
- [ ] Dynamic DAG generation from configuration
- [ ] Stage retry policies
- [ ] Conditional branching (if/else)
- [ ] Loop detection
- [ ] Cycle detection & validation
- [ ] GraphViz export for visual tools
- [ ] Mermaid diagram generation
- [ ] Stage cost estimation
- [ ] Resource requirement analysis
- [ ] Execution history tracking
- [ ] Performance metrics per stage

## Code Structure

### PipelineDAG object
```scala
object PipelineDAG:
  val allStages: List[PipelineStage]
  def executionOrder: List[PipelineStage]
  def parallelStages: Map[Int, List[PipelineStage]]
  def canExecute(stage: PipelineStage, completed: Set[PipelineStage]): Boolean
  def visualizeDAG: String
  def dependencyGraph: String
  def timeline: String
  def executionTrace: String
  def criticalPath: String
```

### PipelineDAGEffects object
```scala
object PipelineDAGEffects:
  def visualize: ZIO[Any, Nothing, Unit]
  def showDependencies: ZIO[Any, Nothing, Unit]
  def showTimeline: ZIO[Any, Nothing, Unit]
  def showExecutionTrace: ZIO[Any, Nothing, Unit]
  def showCriticalPath: ZIO[Any, Nothing, Unit]
  def dryRun: ZIO[Any, Nothing, Unit]
  def showStageDetails: ZIO[Any, Nothing, Unit]
```

### PipelineDAGMain entry point
```scala
object PipelineDAGMain extends ZIOAppDefault:
  def run: ZIO[ZIOAppArgs, Any, Any]
```

## Integration with PipelineOrchestrator

The PipelineDAG and PipelineOrchestrator work together:

- **PipelineDAG**: Models and visualizes the DAG structure
- **PipelineOrchestrator**: Executes the pipeline stages

Both use the same `PipelineStage` types for consistency.

## Example: Complete Analysis Report

```bash
sbt "orchestrator/runMain orchestrator.PipelineDAGMain all" > pipeline-report.txt
```

This generates a comprehensive report containing:
1. ASCII DAG visualization
2. Dependency graph
3. Execution timeline
4. Stage details
5. Execution trace with timing
6. Critical path analysis

Perfect for sharing with team members or documentation.