# Orchestrator Module (Scala 3 + ZIO)

Pure functional pipeline orchestration using ZIO - no Docker dependencies needed!

## Overview

The orchestrator module coordinates the complete data pipeline end-to-end:

```
Publishing → Kafka ✅
     ↓
 Submit → Flink ✅
     ↓
  Wait → Processing ✅
     ↓
   Run → ETL ✅
```

## Architecture

**Pure Functional Design** - Uses ZIO for effect composition and error handling

**Stages:**
1. **Publish to Kafka** - Run preprocessing module to generate and publish test events
2. **Submit Flink Job** - Build and submit Flink streaming job via CLI
3. **Wait for Output** - Poll data lake until Parquet files appear
4. **Run Spark ETL** - Build and submit Spark batch analytics job

## Running the Orchestrator

### Option 1: Run with default 100 events
```bash
sbt "orchestrator/runMain orchestrator.OrchestratorMain"
```

### Option 2: Run with custom event count
```bash
sbt "orchestrator/runMain orchestrator.OrchestratorMain 500"
```

## Implementation Details

### Key Classes

**PipelineOrchestrator** - Core orchestration logic
- `publishToKafka(eventCount)` - Publish events to Kafka topic
- `submitFlinkJob()` - Build and submit Flink job
- `waitForParquetOutput()` - Poll for Parquet files (120s timeout)
- `runSparkETL()` - Build and run Spark ETL
- `runPipeline()` - Orchestrate all stages

**OrchestratorMain** - ZIO App entry point
- Accepts optional event count argument (default: 100)
- Runs the complete pipeline

### Dependencies

- **ZIO 2.0.20** - Pure functional effect system
- **Scala 3.5.2** - Modern Scala features
- **Log4j2** - Structured logging
- **Circe** - JSON processing (minimal)

### Features

✅ **Pure Functional** - ZIO-based, no side effects until execution
✅ **Composable** - Each stage is independently testable
✅ **Error Handling** - Proper error propagation with exit codes
✅ **Logging** - Structured logging with visual indicators (📤, ✅, ⏳, 🚀)
✅ **No Docker** - Runs orchestration locally using system commands

## Configuration

### Environment Variables
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker (default from shared config)
- `ENV` - local or production (affects Paths configuration)

### Polling Settings
Configure in `PipelineOrchestrator.waitForParquetOutput()`:
- `maxWaitSeconds` - Maximum wait time (default: 120s)
- `pollIntervalSeconds` - Check interval (default: 5s)

## Pipeline Flow

```
┌─────────────────────────────────────────────────────┐
│ orchestrator/runMain orchestrator.OrchestratorMain  │
└──────────────────┬──────────────────────────────────┘
                   │
        ┌──────────▼──────────┐
        │ publishToKafka(100) │
        │ sbt preprocessing   │
        │ --> Kafka Topic     │
        └──────────┬──────────┘
                   │
        ┌──────────▼──────────────┐
        │ submitFlinkJob()        │
        │ sbt assembly            │
        │ flink run cli           │
        └──────────┬──────────────┘
                   │
        ┌──────────▼──────────────┐
        │ waitForParquetOutput()  │
        │ Poll data lake dir      │
        │ Max 120 seconds         │
        └──────────┬──────────────┘
                   │
        ┌──────────▼──────────────┐
        │ runSparkETL()           │
        │ sbt assembly            │
        │ spark-submit cli        │
        └──────────┬──────────────┘
                   │
                   ▼
        ✅ Pipeline Complete
```

## Error Handling

Each stage validates exit codes:
- Exit code 0 = Success
- Non-zero = Failure with descriptive error message

If any stage fails:
- Error is logged with visual indicator (❌)
- Pipeline stops immediately
- Non-zero exit code returned to shell

## Example Output

```
================================================================================
🎯 Starting Complete Data Pipeline Orchestration
================================================================================
📤 Publishing 100 events to Kafka...
✅ Successfully published 100 events to Kafka
🔨 Building Flink job...
✅ Built Flink job JAR
📮 Submitting Flink job...
✅ Submitted Flink job with ID: abc123def456
⏳ Waiting for Parquet output in data/streaming/people (max 120s)...
⏳ No Parquet files yet, checking again in 5s...
✅ Found Parquet files in data/streaming/people
🚀 Running Spark ETL batch analytics...
✅ Built Spark ETL JAR
✅ Spark ETL completed successfully
================================================================================
✅ Pipeline orchestration completed successfully!
================================================================================
```

## Testing Individual Stages

Each stage can be tested independently:

```bash
# Test Kafka publishing
sbt "orchestrator/runMain orchestrator.OrchestratorMain" --kafka-only

# Test Flink submission (requires Flink running)
sbt "orchestrator/runMain orchestrator.OrchestratorMain" --flink-only

# Test ETL run
sbt "orchestrator/runMain orchestrator.OrchestratorMain" --etl-only
```

(Note: These options require code extension - not yet implemented)

## Troubleshooting

### "sbt: command not found"
Ensure sbt is installed and in PATH:
```bash
brew install sbt  # macOS
```

### "flink: command not found"
Ensure Flink is installed and in PATH:
```bash
# Set FLINK_HOME and add to PATH
export PATH=$FLINK_HOME/bin:$PATH
```

### "spark-submit: command not found"
Ensure Spark is installed and in PATH:
```bash
# Set SPARK_HOME and add to PATH
export PATH=$SPARK_HOME/bin:$PATH
```

### Kafka connection errors
Ensure Kafka is running:
```bash
# Via docker-compose
docker-compose up -d kafka

# Verify
kafka-topics --bootstrap-server localhost:9092 --list
```

### Parquet output timeout
Increase wait time in code or ensure:
- Kafka has messages (check via Kafka UI)
- Flink job is running (check Flink UI)
- Flink has sufficient resources

## Future Enhancements

- [ ] Add metrics/monitoring endpoints
- [ ] Support partial pipeline execution (--skip-stages)
- [ ] Add retry logic with exponential backoff
- [ ] Integration with workflow schedulers (Airflow, Prefect)
- [ ] REST API for pipeline status/control
- [ ] Distributed orchestration support

## Module Dependency

```
orchestrator (Scala 3.5.2)
    └── shared (Scala 2.13) [for config and schemas]
```

The orchestrator depends on `shared` for Kafka configuration and path management.