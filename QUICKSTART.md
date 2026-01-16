# Quick Start Guide

## 30-Second Setup

### Start Infrastructure
```bash
# Terminal 1: Start Kafka & Flink
docker-compose up -d zookeeper kafka kafka-ui flink-jobmanager flink-taskmanager
```

### Run Complete Pipeline
```bash
# Terminal 2: Run orchestrator
sbt "orchestrator/runMain orchestrator.OrchestratorMain"
```

That's it! The orchestrator will:
1. ✅ Publish 100 events to Kafka
2. ✅ Submit Flink streaming job
3. ✅ Wait for Parquet data (max 120s)
4. ✅ Run Spark ETL

## What Happens

```
Publishing events to Kafka... ⏳
  → 100 PersonEvent messages created
  → Sent to kafka:9093/people-events

Submitting Flink job... ⏳
  → Consumes from Kafka
  → Processes in real-time
  → Writes Parquet files

Waiting for output... ⏳
  → Polling data/streaming/people/
  → Watching for partitioned Parquet files

Running Spark ETL... ⏳
  → Reads Parquet from data lake
  → Aggregates and analyzes
  → Writes results

✅ Pipeline Complete!
```

## Monitor Progress

### Kafka UI
Open http://localhost:8080
- See `people-events` topic
- Inspect message content

### Flink UI
Open http://localhost:8081
- Watch Job status
- See TaskManager metrics
- Check logs

### Data Lake
```bash
# Check generated Parquet files
ls -R data/streaming/people/

# Expected structure:
# data/streaming/people/
#   dt=2025-01-15/
#     hour=14/
#       part-*.parquet
```

## Custom Event Count

```bash
# Generate 500 events instead of 100
sbt "orchestrator/runMain orchestrator.OrchestratorMain 500"
```

## Troubleshooting

### Kafka connection error?
```bash
docker-compose up -d kafka
docker-compose logs kafka  # Check logs
```

### Flink job fails?
```bash
# Check build
sbt "flinkStreaming/compile"

# Check Flink UI for task details
# http://localhost:8081
```

### Timeout waiting for Parquet?
```bash
# Verify Flink job is running
docker-compose logs flink-taskmanager | tail -20

# Increase timeout in code:
# orchestrator/src/main/scala/orchestrator/PipelineOrchestrator.scala
# Change maxWaitSeconds = 120 to 300
```

### Clean and retry
```bash
# Stop everything
docker-compose down -v

# Clean build
sbt clean

# Remove data
rm -rf data/streaming data/output

# Start fresh
docker-compose up -d zookeeper kafka flink-jobmanager flink-taskmanager
sbt "orchestrator/runMain orchestrator.OrchestratorMain"
```

## Detailed Documentation

- **Pipeline Architecture**: See `ORCHESTRATOR.md`
- **Orchestrator Details**: See `orchestrator/README.md`
- **Project Structure**: See `CLAUDE.md`

## Module Breakdown

| Module | Purpose | Technology |
|--------|---------|-----------|
| `preprocessing/` | Generate & publish events | Scala 3, Kafka |
| `flink-streaming/` | Real-time stream processing | Scala 3, Flink |
| `etl/` | Batch analytics | Scala 2.13, Spark |
| `orchestrator/` | Coordinate pipeline | Scala 3, ZIO |
| `shared/` | Shared schemas & config | Scala 2.13 |

## Architecture at a Glance

```
┌──────────────────┐
│  preprocessing   │  Generate & publish events
│  (Scala 3)       │  to Kafka
└────────┬─────────┘
         │
         ▼
    ┌────────┐
    │ Kafka  │  Message broker
    └────┬───┘
         │
         ▼
┌──────────────────┐
│  flink-streaming │  Stream processing
│  (Scala 3)       │  Kafka → Parquet
└────────┬─────────┘
         │
         ▼
  ┌─────────────┐
  │ Parquet     │  Data lake
  │ Data Lake   │  (partitioned)
  └──────┬──────┘
         │
         ▼
┌──────────────────┐
│  etl/            │  Batch analytics
│  (Scala 2.13)    │  Parquet → Results
└──────────────────┘
```

**Orchestration**: `orchestrator/` (Scala 3 + ZIO) coordinates all stages

## Key Features

✅ **Pure Functional** - ZIO-based, composable effects
✅ **Type Safe** - Full Scala 3 type checking
✅ **No Docker** - Orchestration runs on JVM, not in containers
✅ **Multi-Scala** - Scala 3 (modern) + Scala 2.13 (compatibility)
✅ **Production Ready** - Exactly-once semantics, checkpointing, error handling
✅ **Observable** - Structured logging, progress indicators

## Real-World Usage

This setup mirrors production architectures at:
- **Uber** - Real-time analytics with Kafka + Flink
- **Netflix** - Stream processing at scale
- **LinkedIn** - Event-driven pipelines

## Next Steps

1. Run the pipeline: `sbt "orchestrator/runMain orchestrator.OrchestratorMain"`
2. Monitor via web UIs (Kafka: 8080, Flink: 8081)
3. Check generated Parquet: `ls data/streaming/people/`
4. Read `ORCHESTRATOR.md` for deep dive
5. Customize for your use case

---

**Questions?** Check the relevant documentation:
- Architecture: `ORCHESTRATOR.md`
- Orchestrator details: `orchestrator/README.md`
- Project setup: `CLAUDE.md`
- Build config: `build.sbt`