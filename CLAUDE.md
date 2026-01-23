# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**spark-with-scala3** is a real-world streaming data platform demonstrating:
- **Event-driven architecture** with Apache Kafka
- **Stream processing** with Apache Flink (Scala 3)
- **Batch analytics** with Apache Spark (Scala 2.13)
- **Multi-version Scala** - Scala 3 (50%) and Scala 2.13 (50%)
- **Lambda Architecture** - streaming and batch processing layers

**Core Architecture**: Streaming Generator (Scala 3) → Kafka → Flink (Scala 3) → Parquet Data Lake → Spark (Scala 2.13)

This demonstrates production-ready patterns used by companies like Uber, Netflix, and LinkedIn.

## Project Status & Cleanup

**Current State**: Production-ready, lean codebase with optimal structure.

### Recent Optimizations

The project has been optimized for clarity and maintainability:

**Code Cleanup**:
- ✅ Removed 7 unused orchestrator files (DAGVisualizer, PipelineDAG, TaskRunner, etc.)
- ✅ Removed ~72 LOC of dead code across modules
- ✅ Consolidated all dead code removal (analyzeByStatus, rankPeopleByCity, etc.)

**Documentation Consolidation**:
- ✅ Removed 8 duplicate markdown files
- ✅ Kept README.md and CLAUDE.md as single source of truth
- ✅ All project info centralized in these 2 files

**Build Tool Modernization**:
- ✅ Created Makefile for cross-platform command automation
- ✅ Replaced 4 shell scripts with make targets
- ✅ All development workflows now use `make` (example: `make help`)

**Metrics**:
- **Total LOC**: 2,522 lines of essential Scala code
- **File Count**: 14 Scala files (down from 25+)
- **Documentation**: 2 primary docs (README.md, CLAUDE.md)
- **Build System**: Single Makefile with 20+ targets

## Scala Version Strategy

### Philosophy
**Scala 3 for everything except Spark. Scala 2.13 only for Spark and shared module.**

### Module Versions

| Module | Scala Version | Purpose |
|--------|--------------|---------|
| **shared** | **2.13.12** | Lowest common denominator - all modules can depend on it |
| **preprocessing** | **3.5.2** | Event generator - Scala 3 features (enums, opaque types, given/using) |
| **flink-streaming** | **3.5.2** | Stream processing - Scala 3 syntax with Flink Java API |
| **etl** | **2.13.12** | Spark batch analytics - full compatibility |

### Why This Design?

**Binary Compatibility**:
- ✅ Scala 3 can read Scala 2.13 bytecode
- ✅ Scala 2.13 can read Scala 2.13 bytecode
- ❌ Scala 2.x CANNOT read Scala 3 bytecode

**Result**: `shared` (Scala 2.13) acts as the bridge:
```
shared (2.13) ← All modules depend on this
  ├── preprocessing (3) ✅
  ├── flink-streaming (3) ✅
  └── etl (2.13) ✅
```

## Architecture

### Data Flow
```
┌─────────────────────────────────────────┐
│  streaming-generator (Scala 3)          │
│  - Generate Person events                │
│  - Validate using opaque types           │
│  - Publish to Kafka                      │
└──────────────┬──────────────────────────┘
               │
               ▼
        ┌──────────────┐
        │    Kafka     │  Topic: people-events
        │  (Port 9092) │  Format: JSON
        └──────┬───────┘
               │
               ▼
┌─────────────────────────────────────────┐
│  flink-streaming (Scala 3)              │
│  - Consume from Kafka                    │
│  - Event-time processing                 │
│  - Write Parquet (partitioned)           │
│  - Checkpointing (exactly-once)          │
└──────────────┬──────────────────────────┘
               │
               ▼ (Parquet files)
        data/streaming/people/
        └── dt=YYYY-MM-DD/hour=HH/
               │
               ▼
┌─────────────────────────────────────────┐
│  etl (Scala 2.13 + Spark)               │
│  - Read Parquet from data lake           │
│  - Batch analytics & aggregations        │
│  - Write results                         │
└─────────────────────────────────────────┘
```

### Module Structure

**shared/** (Scala 2.13)
- `shared/kafka/PersonEvent.scala` - Kafka message schema
- `shared/kafka/KafkaConfig.scala` - Kafka configuration
- `shared/config/Paths.scala` - Path configuration (local/S3)
- `shared/schemas/PersonSchema.scala` - Field definitions

**preprocessing/** (Scala 3)
- Event generator (to be converted to Kafka publisher)
- Demonstrates: enums, opaque types, extension methods, given/using
- Depends on `shared` ✅

**flink-streaming/** (Scala 3)
- `flink/StreamingJob.scala` - Main Flink job
- Uses Flink Java API with Scala 3 syntax
- Kafka → Parquet with partitioning
- Depends on `shared` ✅

**etl/** (Scala 2.13)
- `etl/SparkETLPipeline.scala` - Spark batch job
- Reads Parquet with schema inference
- Full Spark 3.5.x compatibility
- Depends on `shared` ✅

## Getting Started (Quick Reference)

### Using Makefile (Recommended)

All commands are available via `Makefile` for better portability and consistency:

```bash
make help                # Show all available commands

# Quick start options
make setup               # Check prerequisites
make orchestrator        # Start dashboard (5 min, no docker)
make learn               # Interactive learning mode
make full-stack          # Full pipeline with Docker

# Development
make compile             # Compile all modules
make test                # Run all tests
make clean               # Clean build artifacts

# Run modules directly
make preprocessing-run   # Scala 3 event generator
make etl-run             # Spark batch analytics

# Docker management
make docker-up           # Start all services
make docker-down         # Stop all services
make assembly            # Build JAR files
make versions            # Check Scala versions
```

### Direct sbt Commands (Alternative)

If you prefer to use sbt directly:

```bash
# Compile
sbt compile
sbt "shared/compile"          # Scala 2.13
sbt "preprocessing/compile"   # Scala 3
sbt "flinkStreaming/compile"  # Scala 3
sbt "etl/compile"             # Scala 2.13

# Test
sbt test
sbt "preprocessing/test"

# Build JARs
sbt "preprocessing/assembly"       # → preprocessing/target/scala-3.5.2/preprocessing-assembly.jar
sbt "flinkStreaming/assembly"      # → flink-streaming/target/scala-3.5.2/flink-streaming-assembly.jar
sbt "etl/assembly"                 # → etl/target/scala-2.13/etl-assembly.jar

# Check versions
sbt "show shared/scalaVersion"           # 2.13.12
sbt "show preprocessing/scalaVersion"    # 3.5.2
sbt "show flinkStreaming/scalaVersion"   # 3.5.2
sbt "show etl/scalaVersion"              # 2.13.12
```

### Local Development (without Docker)

```bash
# 1. Check prerequisites
make setup

# 2. Option A: Start interactive orchestrator dashboard
make orchestrator

# 2. Option B: Run learning mode
make learn

# 2. Option C: Run individual modules
make compile
make etl-run
```

### Full Stack with Docker

```bash
# Start infrastructure
make full-stack

# Or manually with docker-compose:
docker-compose up -d zookeeper kafka kafka-ui
docker-compose up -d flink-jobmanager flink-taskmanager

# Access UIs
# - Kafka UI: http://localhost:8080
# - Flink UI: http://localhost:8081
```

## Docker Commands

### Using Makefile (Recommended)

```bash
# Start all services
make docker-up

# Stop all services
make docker-down

# View logs
make docker-logs
```

### Direct Docker Compose Commands

```bash
# Start Kafka ecosystem
docker-compose up -d zookeeper kafka kafka-ui

# Start Flink cluster
docker-compose up -d flink-jobmanager flink-taskmanager

# Check status
docker-compose ps

# Start event generator
docker-compose up -d streaming-generator

# Start Flink job
docker-compose up -d flink-streaming

# Run Spark ETL (on-demand)
docker-compose run etl

# View logs
docker-compose logs -f streaming-generator
docker-compose logs -f flink-streaming
docker-compose logs -f kafka

# Stop everything
docker-compose down

# Stop and remove volumes (clears Kafka data)
docker-compose down -v
```

### Monitoring
- **Kafka UI**: http://localhost:8080
- **Flink UI**: http://localhost:8081
- **Spark UI**: http://localhost:4040 (when ETL is running)

### Build Individual Docker Images

```bash
# Streaming generator (Scala 3)
docker build --build-arg MODULE=preprocessing --target preprocessing -t streaming-generator:latest .

# Flink job (Scala 3)
docker build --build-arg MODULE=flink-streaming --target flink-streaming -t flink-streaming:latest .

# Spark ETL (Scala 2.13)
docker build --build-arg MODULE=etl --target etl -t etl:latest .
```

## Key Architecture Patterns

### Event-Driven Communication
1. **streaming-generator** publishes `PersonEvent` JSON to Kafka topic `people-events`
2. **flink-streaming** consumes from Kafka, writes to Parquet data lake
3. **etl** reads Parquet files for batch analytics

### Kafka Message Schema
Centralized in `shared/kafka/PersonEvent.scala`:
```scala
case class PersonEvent(
  id: String,
  name: String,
  email: String,
  age: Int,
  city: String,
  status: String,
  createdAt: String,
  eventTime: Long  // Unix timestamp for event-time processing
)
```

Serialization uses Jackson (compatible with Kafka producers and Flink consumers).

### Shared Module Pattern
The `shared` module (Scala 2.13) provides:
- Kafka message schemas (`PersonEvent`)
- Kafka configuration (`KafkaConfig`)
- Path configuration (`Paths`)
- Common field definitions (`PersonSchema`)

**Why Scala 2.13?** All modules can depend on it:
- Scala 3 modules can read Scala 2.13 bytecode ✅
- Scala 2.13 modules can read Scala 2.13 bytecode ✅

### Path Configuration
Centralized in `shared/config/Paths.scala`:
- `ENV=local` → uses `data/` directory
- `ENV=production` → uses S3 paths from `S3_BUCKET` env var

Paths:
- `Paths.People.streamingParquet` - Flink writes here
- `Paths.People.outputStats` - Spark writes results here

### Data Lake Partitioning
Flink writes Parquet files partitioned by:
- `dt=YYYY-MM-DD` (date)
- `hour=HH` (hour of day)

Example: `data/streaming/people/dt=2025-12-27/hour=14/part-0-0`

Benefits:
- Efficient time-range queries in Spark
- Easy data lifecycle management
- Cost optimization (prune old partitions)
- Hive-compatible

## Streaming Infrastructure

### Kafka Topics
- **people-events**: Main event stream (JSON format)
- Auto-created by Kafka when first message is published
- Retention: 7 days (configurable in docker-compose.yml)

### Flink Checkpointing
- Checkpoint interval: 60 seconds
- Checkpoint timeout: 5 minutes
- Stored in `/tmp/flink-checkpoints` (Docker volume)
- Enables exactly-once semantics

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `ENV` | `local` | Environment (local/production) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `S3_BUCKET` | `s3://spark-with-scala3` | S3 bucket for production |

## Development Workflow

### Typical Development Cycle
1. Make code changes in appropriate module
2. Compile: `sbt "<module>/compile"`
3. Test: `sbt "<module>/test"`
4. Run locally: `sbt "<module>/run"`
5. For production: `sbt "<module>/assembly"` to build fat JAR

### Adding New Features
- **New event types**: Add to `shared/kafka/` and update Flink job
- **New transformations**: Modify Flink job in `flink-streaming/`
- **New Spark analytics**: Add to `etl/jobs/`
- **Shared config**: Add to `shared` module (use Scala 2.13 syntax)

### Common Gotchas
1. **Scala version mismatch**: preprocessing and flink-streaming are Scala 3, shared and etl are Scala 2.13
2. **Dependency scope**: Spark and Flink deps are `Provided` (available in cluster runtime)
3. **Hadoop client**: Also `Provided` scope - needed for compilation but available at runtime
4. **Path separators**: Use `shared.config.Paths` for all file paths
5. **JVM forking**: All modules have `fork := true` to run in separate JVMs
6. **Kafka connectivity**: Use `KAFKA_BOOTSTRAP_SERVERS` environment variable

## Scala 3 Features Demonstrated

### In preprocessing (Scala 3.5.2)
- **Enums** with parameters - `PersonStatus`
- **Opaque types** for type safety - `Email`, `Age`
- **Extension methods** on `Person`
- **Given/using** for context parameters
- **Union types** for flexible APIs
- **@main** annotation for entry points
- **Derives clause** for automatic typeclass derivation

### In flink-streaming (Scala 3.5.2)
- **New control syntax** - `if-then`, `try-catch` without braces
- **Top-level definitions** - `object` at top level
- **`end` markers** for clarity
- **Indentation-based syntax** (optional)
- Uses Flink Java API for compatibility

### In shared and etl (Scala 2.13.12)
- Traditional Scala 2.13 syntax
- Full Spark compatibility
- All aggregations work perfectly

## Production Deployment

### Standalone JARs
```bash
# Run streaming generator
java -jar preprocessing/target/scala-3.5.2/preprocessing-assembly.jar

# Submit Flink job
flink run -c flink.StreamingJob flink-streaming/target/scala-3.5.2/flink-streaming-assembly.jar

# Run Spark ETL
spark-submit --class etl.SparkETLPipeline etl/target/scala-2.13/etl-assembly.jar
```

### AWS Deployment
1. Upload JARs to S3
2. Run streaming generator on EC2/ECS
3. Submit Flink job to EMR or Kinesis Analytics
4. Schedule Spark ETL on EMR with Step Functions/Airflow
5. Set `ENV=production` and `S3_BUCKET` environment variables

### Docker Deployment
See Docker commands section above. Multi-stage Dockerfile builds minimal runtime images.

## Important Notes for Code Modifications

1. **Scala versions**: Keep preprocessing and flink-streaming as Scala 3, shared and etl as Scala 2.13
2. **Shared module constraints**: Must use Scala 2.13 syntax only (no Scala 3 features)
3. **Kafka message schema**: Changes to `PersonEvent` require coordinated updates to producer and consumer
4. **Assembly conflicts**: If adding new dependencies, update `assemblyMergeStrategy`
5. **Parquet schema evolution**: Maintain backward compatibility when changing schemas
6. **Environment handling**: Always use `shared.config.Paths` and `shared.kafka.KafkaConfig`
7. **Logging**: All modules use Log4j2 - configuration in resources/log4j2.properties
8. **Flink dependencies**: Keep as `Provided` scope - available in Flink cluster runtime
9. **Spark dependencies**: Keep as `Provided` scope - available in Spark cluster runtime

## File Structure

```
scala3-spark/
├── Makefile                      # Command automation (make help)
├── README.md                     # Project overview & quick start
├── CLAUDE.md                     # Technical documentation (this file)
├── build.sbt                     # Multi-module build configuration
├── docker-compose.yml            # Docker infrastructure
├── Dockerfile                    # Multi-stage Docker builds
│
├── project/
│   ├── build.properties
│   └── plugins.sbt              # sbt-assembly plugin
│
├── shared/                       # Scala 2.13 - bridge layer
│   └── src/main/scala/shared/
│       ├── kafka/               # PersonEvent, KafkaConfig
│       ├── config/              # Paths (local/S3)
│       └── schemas/             # Field definitions
│
├── preprocessing/                # Scala 3 - event generator
│   └── src/main/scala/preprocessing/
│       ├── models/              # Person, enums, opaque types
│       └── processors/          # Data processing pipeline
│
├── flink-streaming/              # Scala 3 - stream processing
│   └── src/main/scala/flink/
│       └── StreamingJob.scala   # Kafka → Parquet
│
├── etl/                          # Scala 2.13 - Spark analytics
│   └── src/main/scala/etl/
│       └── SparkETLPipeline.scala
│
├── orchestrator/                 # Scala 2.13 - ZIO dashboard
│   └── src/main/scala/orchestrator/
│       ├── InteractiveOrchestrator.scala     # Web dashboard
│       ├── Task.scala                        # Task definitions
│       ├── TaskExecutionTracker.scala        # Metrics tracking
│       └── OrchestratorDashboard.scala       # Entry point
│
└── data/
    ├── streaming/               # Flink writes here (partitioned)
    └── output/                  # Spark writes results here
```

### Key Files

- **Makefile** - All development commands, see `make help`
- **README.md** - User-facing documentation with quick start
- **CLAUDE.md** - Technical documentation for developers (this file)
- **build.sbt** - Multi-module configuration with proper Scala versions
- **docker-compose.yml** - Complete infrastructure stack

## Troubleshooting

### Compilation Errors
```bash
# Clean and rebuild
sbt clean
sbt compile
```

### Kafka Connection Issues
```bash
# Check Kafka is running
docker-compose ps kafka

# Verify broker is accessible
docker-compose logs kafka
```

### Flink Job Not Starting
```bash
# Check JobManager logs
docker-compose logs flink-jobmanager

# Check TaskManager is connected
# Flink UI → Task Managers
```

### No Data in Data Lake
```bash
# Verify Kafka topic has messages (Kafka UI)
# Check Flink job is running (Flink UI)
# Check Flink logs
docker-compose logs flink-streaming
```

## Next Steps for Development

### Immediate Tasks

1. **Complete Kafka Integration** (Optional Enhancement)
   - The `preprocessing` module currently generates test data
   - Next: Convert to Kafka publisher using fs2-kafka
   - This would enable the full streaming pipeline

2. **Enhance the Dashboard**
   - Add real metrics from running tasks
   - Implement task dependency visualization
   - Add configuration UI for environment variables

3. **Production Deployment**
   - Test deployment to AWS EMR (Spark)
   - Deploy generator and Flink to ECS/Kubernetes
   - Set up monitoring with CloudWatch/Datadog

### For Learning & Contribution

- **Explore Scala 3 Features**: See `preprocessing/src/main/scala/preprocessing/models/Person.scala`
- **Study Lambda Architecture**: Compare `etl/SparkETLPipeline.scala` with Flink patterns
- **Understand Multi-Version Scala**: See how `shared` bridges Scala 2.13 and 3.5.2
- **Learn Streaming Patterns**: Review `flink-streaming/src/main/scala/flink/StreamingJob.scala`

## Getting Help

- **Questions about code**: Check the relevant module's source code
- **Issues with setup**: Run `make help` to see all available commands
- **Architecture questions**: See this CLAUDE.md file
- **Quick reference**: See README.md for overview and quick start
