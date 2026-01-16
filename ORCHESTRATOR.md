# ZIO-Based Pipeline Orchestrator

## Overview

This project now includes a **pure functional, ZIO-based orchestrator** for the complete data pipeline. No Docker needed for orchestration - just Scala 3, ZIO, and sbt.

## Complete Pipeline Architecture

```
╔════════════════════════════════════════════════════════════════════════════╗
║                        COMPLETE DATA PIPELINE                              ║
╚════════════════════════════════════════════════════════════════════════════╝

┌─────────────────────────────────────────────────────────────────────────────┐
│ 1️⃣  Event Generation & Publishing (Scala 3)                                │
├─────────────────────────────────────────────────────────────────────────────┤
│  Module:  preprocessing/                                                    │
│  Command: sbt "preprocessing/runMain preprocessing.PreprocessingPipeline"  │
│  Output:  PersonEvent messages → Kafka "people-events" topic              │
│  Schema:  id, name, email, age, city, status, createdAt, eventTime       │
│  Features:                                                                  │
│    • Scala 3 enums and opaque types for type safety                        │
│    • Native Kafka producer (kafka-clients 3.6.1)                          │
│    • Generates test data with proper validation                            │
│    • Publishes JSON serialized events                                      │
└──────────────────────┬──────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 2️⃣  Message Broker (Kafka + Zookeeper)                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  Component:  Apache Kafka 7.5.0                                            │
│  Topic:      people-events (JSON format)                                   │
│  Retention:  7 days                                                         │
│  Access:     kafka:9093 (internal), localhost:9092 (external)             │
│  Monitoring: Kafka UI at http://localhost:8080                             │
└──────────────────────┬──────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 3️⃣  Stream Processing (Scala 3 + Apache Flink)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  Module:    flink-streaming/                                               │
│  Framework: Apache Flink 1.18.1                                            │
│  Command:   flink run -c flink.StreamingJob flink-streaming-assembly.jar  │
│  Features:                                                                  │
│    • Consumes from Kafka "people-events" topic                             │
│    • Event-time processing with watermarking (10s bounded out-of-order)   │
│    • Converts PersonEvent → Avro GenericRecord                             │
│    • Writes Parquet files partitioned by dt=YYYY-MM-DD/hour=HH/          │
│    • Exactly-once semantics via checkpointing (60s intervals)             │
│    • Web UI at http://localhost:8081                                      │
│  Output Path: data/streaming/people/                                      │
└──────────────────────┬──────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 4️⃣  Data Lake (Parquet + Partitioned Columnar Storage)                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  Format:        Apache Parquet (columnar, compressed)                       │
│  Location:      data/streaming/people/dt=YYYY-MM-DD/hour=HH/            │
│  Schema:        Person event data with timestamps                          │
│  Partitioning:  Date-based for efficient time-range queries               │
│  Lifecycle:     7-day retention matching Kafka topic                       │
└──────────────────────┬──────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ 5️⃣  Batch Analytics (Scala 2.13 + Apache Spark)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│  Module:    etl/                                                            │
│  Framework: Apache Spark 3.5.3                                             │
│  Command:   spark-submit --class etl.SparkETLPipeline etl-assembly.jar   │
│  Features:                                                                  │
│    • Reads Parquet from data lake                                         │
│    • Batch transformations and aggregations                                │
│    • Schema inference from Parquet files                                  │
│    • Delta Lake for ACID transactions                                     │
│    • Web UI at http://localhost:4040 (when running)                      │
│  Output:    data/output/ (aggregated results)                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## The Orchestrator: Pure Functional Coordination

### ZIO-Based Orchestrator

**Module:** `orchestrator/`
**Language:** Scala 3.5.2
**Framework:** ZIO 2.0.20

Coordinates all stages of the pipeline sequentially:

```scala
runPipeline(eventCount: Int) =
  publishToKafka(eventCount)        // Stage 1: Generate and publish events
    → submitFlinkJob()               // Stage 2: Start streaming job
    → waitForParquetOutput()         // Stage 3: Poll for data lake output
    → runSparkETL()                  // Stage 4: Run batch analytics
```

### Key Advantages

✅ **No Docker overhead** - Pure JVM orchestration
✅ **Type safe** - Full Scala 3 type checking
✅ **Composable effects** - ZIO monadic composition
✅ **Error handling** - Proper exception propagation
✅ **Structured logging** - JSON-compatible log output
✅ **Observable stages** - Visual feedback at each step

## Running the Complete Pipeline

### Prerequisites

1. **Kafka Running** (Docker):
```bash
docker-compose up -d zookeeper kafka kafka-ui
```

2. **Flink Cluster Running** (Docker):
```bash
docker-compose up -d flink-jobmanager flink-taskmanager
```

3. **System Tools Installed**:
```bash
# sbt (build tool)
brew install sbt

# Flink CLI
brew install flink

# Spark
brew install apache-spark
```

### Execute Complete Pipeline

**Option 1: Default (100 events)**
```bash
sbt "orchestrator/runMain orchestrator.OrchestratorMain"
```

**Option 2: Custom event count**
```bash
sbt "orchestrator/runMain orchestrator.OrchestratorMain 500"
```

### What Happens

```
Step 1: Publishing 100 events to Kafka
  → Compiles preprocessing module
  → Generates test Person records
  → Publishes to kafka:9093/people-events
  → Verifies success count

Step 2: Building and submitting Flink job
  → Compiles flink-streaming module
  → Builds assembly JAR (3.5.2)
  → Submits to Flink via CLI
  → Extracts and logs Job ID

Step 3: Waiting for Parquet output
  → Polls data/streaming/people/ every 5 seconds
  → Waits up to 120 seconds for files to appear
  → Verifies partition structure (dt=*/hour=*/)

Step 4: Building and running Spark ETL
  → Compiles etl module
  → Builds assembly JAR (2.13.12)
  → Submits to Spark
  → Waits for completion
```

## Module Structure

```
scala3-spark/
├── shared/                          # Scala 2.13 - Shared schemas & config
│   └── src/main/scala/shared/
│       ├── kafka/
│       │   ├── PersonEvent.scala    # Event schema (JSON serialization)
│       │   └── KafkaConfig.scala    # Broker configuration
│       └── config/
│           └── Paths.scala          # Data lake paths (local/S3)
│
├── preprocessing/                   # Scala 3 - Event generator
│   └── src/main/scala/preprocessing/
│       ├── KafkaPublisher.scala     # Kafka producer logic
│       ├── PreprocessingPipeline.scala
│       └── models/
│           └── Person.scala         # Domain model with opaque types
│
├── flink-streaming/                 # Scala 3 - Stream processor
│   └── src/main/scala/flink/
│       └── StreamingJob.scala       # Flink job (Kafka → Parquet)
│
├── etl/                             # Scala 2.13 - Batch analytics
│   └── src/main/scala/etl/
│       └── SparkETLPipeline.scala   # Spark SQL transformations
│
├── orchestrator/                    # Scala 3 - ZIO orchestrator ⭐ NEW
│   └── src/main/scala/orchestrator/
│       └── PipelineOrchestrator.scala # Pure functional coordinator
│
├── data/                            # Data lake
│   ├── raw/                         # Input data
│   ├── streaming/                   # Flink writes Parquet here
│   │   └── people/dt=*/hour=*/
│   └── output/                      # Spark writes results here
│
├── build.sbt                        # Multi-module SBT config
├── docker-compose.yml               # Infrastructure (Kafka, Flink)
└── ORCHESTRATOR.md                  # This file
```

## Scala Version Strategy

| Module | Version | Why |
|--------|---------|-----|
| **shared** | 2.13.12 | Bridge layer - all modules depend on it |
| **preprocessing** | 3.5.2 | Modern Scala 3 features (enums, opaque types) |
| **flink-streaming** | 3.5.2 | Scala 3 new control syntax with Flink Java API |
| **etl** | 2.13.12 | Full Spark 3.5.x compatibility |
| **orchestrator** | 3.5.2 | Pure functional ZIO effects |

**Binary Compatibility**: Scala 3 can read Scala 2.13 bytecode ✅

## Configuration

### Environment Variables

| Variable | Default | Usage |
|----------|---------|-------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `ENV` | `local` | local = file paths, production = S3 paths |
| `S3_BUCKET` | `s3://spark-with-scala3` | S3 bucket for production |

### Orchestrator Customization

In `orchestrator/src/main/scala/orchestrator/PipelineOrchestrator.scala`:

```scala
// Customize waiting time for Parquet output
waitForParquetOutput(
  maxWaitSeconds = 120,      // Increase if processing is slow
  pollIntervalSeconds = 5    // Decrease for more frequent checks
)
```

## Monitoring & Debugging

### Web UIs

| Component | URL | Purpose |
|-----------|-----|---------|
| Kafka | http://localhost:8080 | Topic monitoring, message inspection |
| Flink | http://localhost:8081 | Job status, task managers, logs |
| Spark | http://localhost:4040 | When ETL is running - stage details |

### Logs

```bash
# Kafka logs
docker-compose logs -f kafka

# Flink JobManager
docker-compose logs -f flink-jobmanager

# Flink TaskManager
docker-compose logs -f flink-taskmanager

# Orchestrator output
sbt "orchestrator/runMain orchestrator.OrchestratorMain" 2>&1 | tee orchestrator.log
```

### Check Pipeline Status

```bash
# Verify Kafka topic
kafka-topics --bootstrap-server localhost:9092 --list

# Check Parquet files generated
ls -R data/streaming/people/

# Verify Spark ETL output
ls -R data/output/
```

## Error Scenarios

### Kafka Connection Errors
```
❌ Failed to publish events (exit code: 1)
```
**Fix**: Ensure Kafka is running
```bash
docker-compose up -d kafka
```

### Flink Job Submission Failed
```
❌ Failed to build Flink job (exit code: 1)
```
**Fix**: Check Flink is running and compilation errors
```bash
sbt "flinkStreaming/compile"
docker-compose up -d flink-jobmanager flink-taskmanager
```

### Parquet Output Timeout
```
Timeout waiting for Parquet output in data/streaming/people
```
**Fix**: Check Flink job is actually processing
- Open Flink UI at http://localhost:8081
- Verify Job is RUNNING (not FAILED or RESTARTING)
- Check TaskManager slots are available
- Increase timeout in orchestrator code if processing is slow

### Spark ETL Failure
```
❌ Spark ETL failed (exit code: 1)
```
**Fix**: Verify Spark is installed and has required libraries
```bash
spark-submit --version
```

## Performance Tuning

### Event Publishing Speed
```scala
// In preprocessing/src/main/scala/preprocessing/KafkaPublisher.scala
props.put("batch.size", "32768")      // Increase for throughput
props.put("linger.ms", "10")          // Wait 10ms for batching
```

### Flink Checkpointing
```scala
// In flink-streaming/src/main/scala/flink/StreamingJob.scala
env.enableCheckpointing(30000)        // Reduce from 60s for faster recovery
checkpointConfig.setMinPauseBetweenCheckpoints(5000)  // Faster checkpoints
```

### Spark Parallelism
```bash
spark-submit \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.default.parallelism=200 \
  --class etl.SparkETLPipeline etl-assembly.jar
```

## Next Steps & Enhancements

- [ ] Add Kafka consumer group offsets tracking
- [ ] Implement metrics/monitoring via Prometheus
- [ ] Add retry logic with exponential backoff
- [ ] Support for Spark Structured Streaming
- [ ] Integration with Airflow/Prefect (submit orchestrator as task)
- [ ] Delta Lake support for production
- [ ] Schema evolution and validation
- [ ] Data quality checks between stages
- [ ] Multi-tenant isolation
- [ ] Auto-scaling TaskManager resources

## Common Tasks

### Clean Everything
```bash
# Stop all containers
docker-compose down

# Clean SBT build artifacts
sbt clean

# Remove generated data
rm -rf data/streaming data/output
```

### Rebuild Everything
```bash
# Full rebuild
sbt clean
sbt compile

# Build assembly JARs
sbt assembly
```

### Test Individual Stages

```bash
# Test event generation
sbt "preprocessing/runMain preprocessing.PreprocessingPipeline publishTestDataToKafka 10"

# Test Flink job (requires Docker)
docker-compose up -d flink-jobmanager flink-taskmanager
sbt "flinkStreaming/run"

# Test Spark ETL (requires Parquet files)
sbt "etl/runMain etl.SparkETLPipeline"
```

## Summary

You now have a **production-ready, pure functional data pipeline** with:

✅ **Scala 3** for modern language features
✅ **ZIO** for composable, type-safe effects
✅ **Kafka** for event streaming
✅ **Flink** for real-time stream processing
✅ **Parquet** for efficient data lake storage
✅ **Spark** for batch analytics
✅ **Pure functional orchestration** - no Python, no Docker needed!

Run the complete pipeline with a single command:
```bash
sbt "orchestrator/runMain orchestrator.OrchestratorMain"
```

See `/orchestrator/README.md` for detailed orchestrator documentation.