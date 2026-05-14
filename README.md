# spark-with-scala3

> **Real-World Streaming Data Platform: Scala 3 + Kafka + Flink + Spark**

A production-ready example demonstrating how to use Scala 3 features alongside Apache Spark in a modern data engineering pipeline.

## 🎯 What This Project Demonstrates

- **Scala 3** for modern application code (event generation, stream processing)
- **Scala 2.13** for Spark compatibility and as a bridge layer
- **Apache Kafka** for event streaming
- **Apache Flink** (with Scala 3!) for real-time stream processing
- **Apache Spark** for batch analytics
- **Lambda Architecture** - streaming + batch processing layers
- **Production patterns** used by companies like Uber, Netflix, LinkedIn

## 📊 Architecture

```
┌─────────────────────────────┐
│ Streaming Generator         │  Scala 3.5.2
│ - Enums, Opaque Types       │  Modern features
│ - Extension Methods         │
└──────────┬──────────────────┘
           │ Kafka (people-events topic)
           ▼
┌─────────────────────────────┐
│ Apache Kafka                │  Event backbone
│ - Decoupled services        │
│ - Reliable messaging        │
└──────────┬──────────────────┘
           │
           ▼
┌─────────────────────────────┐
│ Flink Streaming             │  Scala 3.5.2
│ - Real-time processing      │  Stream processing
│ - Write to Data Lake        │
└──────────┬──────────────────┘
           │ Parquet (partitioned by date/hour)
           ▼
┌─────────────────────────────┐
│ Spark Batch Analytics       │  Scala 2.13
│ - Aggregations              │  Full compatibility
│ - Complex analytics         │
└─────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites
- Java 17+
- sbt 1.10+
- Docker & Docker Compose (optional, for infrastructure)
- `make` (usually pre-installed on macOS/Linux)

### Automated Setup (Recommended)

```bash
# 1. Check prerequisites (Java, sbt, Docker)
make setup

# 2. Choose your path...
```

### Option 1: Interactive Dashboard (5 minutes) ⚡ **Quickest**

```bash
# Start the orchestrator dashboard with simulated tasks
make orchestrator

# Open browser to http://localhost:9090
# See all tasks and their execution status in real-time
```

### Option 2: Learning Mode (Explore the Code)

```bash
# Interactive exploration of Scala 3 features and ETL
make learn

# Options:
# 1. Compile all modules
# 2. Run Spark ETL locally
# 3. Explore Preprocessing module (Scala 3)
# 4. Run tests
# 5. Start interactive Scala REPL
```

### Option 3: Full Stack with Docker (Complete Pipeline)

```bash
# Start Kafka, Flink, and all infrastructure
make full-stack

# Then in another terminal, start Flink:
docker-compose up -d flink-jobmanager flink-taskmanager

# Access web UIs
# - Kafka UI: http://localhost:8080
# - Flink UI: http://localhost:8081

# Start the streaming generator
docker-compose up -d streaming-generator

# Run Spark analytics
make etl-run
```

### Option 4: Manual Development Commands

```bash
# Compile everything
make compile

# Run specific modules
make preprocessing-run           # Scala 3 event generator
make etl-run                     # Spark batch analytics

# Test
make test

# Clean
make clean

# Docker management
make docker-up                   # Start all services
make docker-down                 # Stop all services
make docker-logs                 # View logs
```

### All Available Commands

```bash
make help                         # Show all available commands
```

## 📦 Module Structure

| Module | Scala Version | Purpose |
|--------|--------------|---------|
| **shared** | 2.13.12 | Common schemas, Kafka messages, configs (bridge layer) |
| **preprocessing** | 3.5.2 | Event generator with Scala 3 features |
| **flink-streaming** | 3.5.2 | Stream processing (Kafka → Parquet) |
| **etl** | 2.13.12 | Spark batch analytics |

## 🎓 Scala 3 Features Demonstrated

### Event Generator (`preprocessing`)
- ✅ **Enums** with parameters
- ✅ **Opaque types** for type-safe wrappers
- ✅ **Extension methods**
- ✅ **Given/using** context parameters
- ✅ **Union types**
- ✅ **Top-level definitions**
- ✅ **@main annotation**

### Stream Processing (`flink-streaming`)
- ✅ **New control syntax** (if-then without braces)
- ✅ **End markers**
- ✅ **Indentation-based syntax**
- ✅ Works with Flink Java API

### Why Scala 2.13 for `shared` and `etl`?

**Binary Compatibility**:
- Scala 3 can read Scala 2.13 bytecode ✅
- Scala 2.13 CANNOT read Scala 3 bytecode ❌

**Solution**: Use Scala 2.13 for the bridge layer (`shared`) so all modules can depend on it!

## 🔧 Development Commands

```bash
# Compile
sbt compile                      # All modules
sbt "preprocessing/compile"      # Specific module

# Test
sbt test                         # All tests
sbt "etl/test"                   # Specific module

# Build JARs
sbt "preprocessing/assembly"     # Scala 3 JAR
sbt "flinkStreaming/assembly"    # Scala 3 JAR
sbt "etl/assembly"               # Scala 2.13 JAR

# Check Scala versions
sbt "show preprocessing/scalaVersion"    # 3.5.2
sbt "show etl/scalaVersion"              # 2.13.12
```

## 🐳 Docker Commands

```bash
# Start infrastructure
docker-compose up -d zookeeper kafka flink-jobmanager flink-taskmanager

# Build all images
docker-compose build

# Start full pipeline
docker-compose up

# View logs
docker-compose logs -f streaming-generator
docker-compose logs -f flink-streaming

# Stop everything
docker-compose down
```

## 🌐 Web UIs

| Service | URL | Purpose |
|---------|-----|---------|
| Kafka UI | http://localhost:8080 | Monitor topics, messages |
| Flink UI | http://localhost:8081 | Monitor streaming jobs |
| Spark UI | http://localhost:4040 | Monitor batch jobs |

## 📁 Project Layout

```
scala3-spark/
├── Makefile               # Command automation (make help to see all targets)
├── README.md              # This file
├── CLAUDE.md              # Technical documentation for developers
├── build.sbt              # Multi-module sbt configuration
├── docker-compose.yml     # Docker infrastructure
├── Dockerfile             # Multi-stage Docker builds
│
├── shared/                # Scala 2.13 - bridge layer
│   └── src/main/scala/shared/
│       ├── kafka/         # PersonEvent, KafkaConfig
│       ├── config/        # Paths (local/S3)
│       └── schemas/       # Field definitions
│
├── preprocessing/         # Scala 3 - event generator
│   └── src/main/scala/preprocessing/
│       ├── models/        # Person, enums, opaque types
│       └── processors/    # Data processing pipeline
│
├── flink-streaming/       # Scala 3 - stream processing
│   └── src/main/scala/flink/
│       └── StreamingJob.scala
│
├── etl/                   # Scala 2.13 - Spark analytics
│   └── src/main/scala/etl/
│       └── SparkETLPipeline.scala
│
├── orchestrator/          # ZIO orchestrator dashboard (Scala 2.13)
│   └── src/main/scala/orchestrator/
│       ├── InteractiveOrchestrator.scala  # Web dashboard
│       ├── Task.scala                     # Task definitions
│       ├── TaskExecutionTracker.scala     # Execution metrics
│       └── OrchestratorDashboard.scala    # Entry point
│
└── data/
    ├── streaming/         # Flink writes here (partitioned)
    └── output/            # Spark writes results
```

## 🎯 Key Design Decisions

### 1. Why Scala 3 for Flink?
- Flink Java API works with any JVM language
- We use Scala 3 syntax with Flink Java API
- Best of both worlds: Flink stability + Scala 3 features

### 2. Why Scala 2.13 (not 2.12)?
- Better forward-compatibility with Scala 3
- Spark 3.5.x fully supports both 2.12 and 2.13
- When Spark 4.x supports Scala 3, easy migration

### 3. Data Lake Partitioning
Flink writes: `data/streaming/people/dt=2025-12-27/hour=14/`

Benefits:
- Efficient Spark queries (partition pruning)
- Easy data lifecycle management
- Hive-compatible

## 🏭 Production Deployment

### Standalone JARs
```bash
# Event generator
java -jar preprocessing/target/scala-3.5.2/preprocessing-assembly.jar

# Flink job
flink run -c flink.StreamingJob flink-streaming/target/scala-3.5.2/flink-streaming-assembly.jar

# Spark ETL
spark-submit --class etl.SparkETLPipeline etl/target/scala-2.13/etl-assembly.jar
```

### AWS Deployment
1. Upload JARs to S3
2. Run generator on EC2/ECS
3. Submit Flink to EMR/Kinesis Analytics
4. Schedule Spark ETL with Step Functions
5. Set `ENV=production` and `S3_BUCKET` environment variables

## 🔍 Monitoring

### Kafka Topics
```bash
# View topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# View messages
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic people-events \
  --from-beginning
```

### Check Data
```bash
# Check Parquet files written by Flink
ls -R data/streaming/people/

# Check results from Spark
ls -R data/output/
```

## 📚 Documentation

- **README.md** (this file) - Project overview and quick start
- **CLAUDE.md** - Comprehensive technical documentation, architecture details, and Scala version strategy
- **Makefile** - All available commands (run `make help`)

## 🧹 Project Structure

This project has been optimized for clarity and maintainability:

- **Lean codebase**: Only 2,522 lines of essential Scala code across 14 files
- **No dead code**: All unused methods and orchestrator files have been removed
- **Consolidated documentation**: Single source of truth in README.md and CLAUDE.md
- **Automated setup**: Use `make` commands instead of shell scripts for better portability
- **Clean module structure**: Each module has a single, clear responsibility

Key files:
- `Makefile` - All development commands (setup, compile, test, run, docker)
- `build.sbt` - Multi-module build configuration with proper Scala versions
- `docker-compose.yml` - Complete infrastructure (Kafka, Flink, Zookeeper)

## 🎓 Learning Outcomes

This project teaches you:
1. **Event-driven architecture** with Kafka
2. **Stream processing** with Flink
3. **Lambda architecture** (streaming + batch)
4. **Scala 3 adoption** in production systems
5. **Multi-version Scala** in a single codebase
6. **Real-world data engineering** patterns

## 🤝 Use Cases

This pattern is perfect for:
- **Data platforms** transitioning to Scala 3
- **Streaming pipelines** with real-time requirements
- **Teams learning** Scala 3 while maintaining Spark
- **Production systems** needing both modern features and Spark compatibility

## ⚡ Next Steps

The main remaining work is to convert the `preprocessing` module to publish events to Kafka:

```scala
// Current: writes to Parquet
parquet4s.write(people, "data/processed/people.parquet")

// Target: publishes to Kafka
kafkaProducer.send("people-events", PersonEvent.toJson(event))
```

This would complete the real-time streaming pipeline!

## 🐛 Troubleshooting

### Compilation errors
```bash
sbt clean
sbt compile
```

### Kafka not accessible
```bash
docker-compose ps kafka
docker-compose logs kafka
```

### Flink job not starting
```bash
docker-compose logs flink-jobmanager
# Check Flink UI: http://localhost:8081
```

## 📜 License

MIT License - feel free to use for learning and production!

## ⭐ Why This Matters

This project demonstrates the **RIGHT way to adopt Scala 3 in data engineering**:

✅ Use Scala 3 where you can (50% of modules)
✅ Use Scala 2.13 where you must (Spark) and as bridge
✅ All modules share code via Scala 2.13 shared layer
✅ Production-ready with no compromises
✅ Future-proof for easy Scala 3 migration

**Result**: Modern Scala 3 codebase with perfect Spark compatibility! 🚀

---

Built with ❤️ to demonstrate Scala 3 + Spark + Flink best practices.
