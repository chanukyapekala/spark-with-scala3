# Building a Modern Data Platform: Scala 3 + Kafka + Flink + Spark

## 🎯 Project Overview

This project demonstrates a **production-ready streaming data platform** that combines the best of modern data engineering:

- **Real-time streaming** with Apache Flink
- **Batch analytics** with Apache Spark
- **Modern language features** with Scala 3
- **Event-driven architecture** with Kafka

It's the kind of system used by companies like Uber, Netflix, and LinkedIn to process billions of events daily.

## 🏗️ Architecture at a Glance

```
┌─────────────────────────────────────────────────────┐
│  Event Generator (Scala 3)                          │
│  ├─ Generate synthetic Person events                │
│  ├─ Validate data with opaque types                 │
│  └─ Publish to Kafka                                │
└────────────────────┬────────────────────────────────┘
                     │ (Kafka Topic: people-events)
                     │ JSON format messages
                     ▼
┌─────────────────────────────────────────────────────┐
│  Apache Kafka (Message Broker)                      │
│  └─ Decouples producers from consumers              │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│  Flink Streaming Job (Scala 3)                      │
│  ├─ Consume from Kafka (real-time)                  │
│  ├─ Event-time processing with 1-hour windows      │
│  └─ Write partitioned Parquet to data lake          │
│     (Format: dt=YYYY-MM-DD/hour=HH)                 │
└────────────────────┬────────────────────────────────┘
                     │ (Parquet files on disk/S3)
                     │ Organized by date & hour
                     ▼
┌─────────────────────────────────────────────────────┐
│  Spark Batch Analytics (Scala 2.13)                 │
│  ├─ Read Parquet data lake                          │
│  ├─ Aggregations (count, avg, stddev, etc.)         │
│  ├─ Data enrichment (categorization, domains, etc.) │
│  └─ Write analytical results                        │
└─────────────────────────────────────────────────────┘

                    Lambda Architecture
         (Real-time + Batch = Complete View)
```

## 📦 The 5 Modules

### 1. **shared** (Scala 2.13 - The Bridge)
The glue that holds everything together.

**Why Scala 2.13?**
- All modules can depend on it
- Scala 3 can read Scala 2.13 bytecode ✅
- Scala 2.13 can read Scala 2.13 bytecode ✅
- Scala 2 CANNOT read Scala 3 bytecode ❌

**What it contains:**
```
├─ PersonEvent          (Kafka message schema)
├─ KafkaConfig          (Connection settings)
├─ Paths                (Local/S3 path configuration)
└─ PersonSchema         (Field definitions)
```

### 2. **preprocessing** (Scala 3 - Event Generation)
Generates synthetic event data and showcases Scala 3 features.

**Key Scala 3 Features Demonstrated:**
- **Enums with parameters**: `PersonStatus.Suspended(reason, until)`
- **Opaque types**: Type-safe wrappers (`Email`, `Age`)
- **Extension methods**: Clean API for domain objects
- **Given/using**: Context parameters for dependency injection

**What it does:**
```scala
Person.create("Alice", "alice@example.com", 25, "NYC")
  .fold(
    error => println(s"Invalid: $error"),
    person => publishToKafka(person)
  )
```

### 3. **flink-streaming** (Scala 3 - Stream Processing)
Real-time processing engine that transforms events into structured data.

**Architecture:**
```
Kafka → Deserialize → Validate → Time Windows → Aggregate → Parquet
(JSON)    (GenericRecord)         (1 hour)     (count,avg,  (Partitioned)
                                              stddev...)
```

**Key capabilities:**
- Exactly-once semantics (checkpointing)
- Event-time processing (not wall-clock time)
- Automatic partition creation by date/hour
- Fault tolerance with state backends

### 4. **etl** (Scala 2.13 - Batch Analytics)
Spark jobs that run analytics on the data lake.

**What it computes:**
```
Statistics by city:
├─ Total people per city
├─ Average age distribution
├─ Min/Max age ranges
├─ Standard deviation analysis
└─ Status distribution

Data enrichment:
├─ Age group categorization (Minor, Adult, Senior)
├─ Email domain extraction
└─ Active status flags
```

### 5. **orchestrator** (Scala 2.13 + ZIO - Workflow Control)
Interactive dashboard for monitoring and executing the entire pipeline.

**Why ZIO?**
- Composable concurrent effects
- Type-safe error handling
- Non-blocking async operations
- Clean state management

**Features:**
- Real-time task execution status
- Task metrics and logs
- Web dashboard (http://localhost:9090)
- REST API for programmatic control

## 🔑 Key Architectural Decisions

### Decision 1: Lambda Architecture
```
Speed Layer (Real-time)      Batch Layer (Accuracy)
    ↓                              ↓
  Flink                         Spark
    ↓                              ↓
  Parquet ← → Merge → Analytics View
```

**Why?**
- Real-time: Flink processes events as they arrive
- Batch: Spark corrects and enriches using complete datasets
- Combined: Users get both speed AND accuracy

### Decision 2: Multi-Version Scala
| Module | Version | Reason |
|--------|---------|--------|
| shared | 2.13 | Bridge layer - all modules depend on it |
| preprocessing | 3.5.2 | Modern features for event generation |
| flink-streaming | 3.5.2 | Scala 3 syntax with Flink Java API |
| etl | 2.13 | Full Spark compatibility |
| orchestrator | 2.13 | Works with all modules |

**Why not all Scala 3?**
- Spark doesn't support Scala 3 yet
- Kafka ecosystem primarily uses Scala 2.13
- **Solution**: Use Scala 2.13 as bridge, Scala 3 where possible

### Decision 3: Parquet as Data Lake Format
**Benefits:**
- Columnar format (efficient for analytics)
- Schema preservation
- Compression built-in
- Works seamlessly with Spark/Flink
- Cloud-native (S3, GCS compatible)

### Decision 4: Event-Time Processing
```
Wall-clock time:  10:00 → 10:01 → 10:02
                   ↓
Event time:       09:58 → 09:59 → 10:00
(When event happened in user's system)

Flink uses EVENT time for windows = correct results!
```

## 📊 Code Quality Metrics

After optimization:
- **Lines of Code**: 2,522 (lean, focused)
- **Scala Files**: 14 (minimal, essential)
- **Documentation**: 2 files (consolidated)
- **Build Tool**: Makefile (cross-platform)
- **Dead Code**: 0 (cleaned up)

## 🚀 Getting Started (3 Options)

### Option 1: 5-Minute Dashboard
```bash
make setup        # Check prerequisites
make orchestrator # Start interactive dashboard
# Open http://localhost:9090
```

### Option 2: Learn the Code
```bash
make learn        # Interactive exploration of Scala 3 features
# Choose from: compile, run examples, tests, REPL
```

### Option 3: Full Stack
```bash
make full-stack   # Start Kafka, Zookeeper, Flink
sbt "etl/run"     # Run Spark analytics
```

## 💡 Key Takeaways

### For Data Engineers
✅ Lambda architecture combines real-time speed with batch accuracy
✅ Kafka decouples data producers from consumers
✅ Flink provides exactly-once semantics (no data loss)
✅ Spark excels at analytical queries on the data lake

### For Scala Developers
✅ Scala 3 features (enums, opaque types) improve type safety
✅ ZIO provides elegant concurrent effect composition
✅ Multi-version Scala can work if architected carefully
✅ Functional programming shines in data pipelines

### For System Design
✅ Event-driven architecture scales horizontally
✅ Partitioning by time enables efficient data management
✅ Decoupling layers (Kafka) improves resilience
✅ Orchestration layer (ZIO) handles complex workflows

## 🎓 What Makes This Project Different

Most tutorials show:
- Single-language systems (not multi-version)
- Only real-time OR batch (not both)
- Toy examples (not production patterns)

**This project shows:**
- How to manage Scala 2.13 + 3 together
- Complete Lambda architecture implementation
- Real data engineering patterns at scale
- Modern Scala features in production context

## 🔗 Production Deployment

This architecture scales to billions of events:

```
Cloud Deployment (AWS/GCP/Azure):
├─ Event Generator: ECS/EC2 (can scale horizontally)
├─ Kafka: Managed (MSK, Confluent Cloud)
├─ Flink: EMR / Kubernetes cluster
├─ Spark: EMR / Databricks (on-demand)
└─ Data Lake: S3 / GCS (cost-effective storage)
```

**Cost Optimization:**
- Kafka: Partitions by time = delete old data easily
- Flink: Checkpoints enable recovery without reprocessing
- Spark: Partition pruning reduces scan time
- Result: Only pay for data you use

---

## 📖 Learn More

**GitHub**: [chanukyapekala/spark-with-scala3](https://github.com/chanukyapekala/spark-with-scala3)

**Key Files**:
- `README.md` - Quick start guide
- `CLAUDE.md` - Technical deep dive
- `Makefile` - All development commands

**Technologies Used**:
- Apache Kafka (Event streaming)
- Apache Flink (Stream processing)
- Apache Spark (Batch analytics)
- Scala 3.5.2 & 2.13.12 (JVM language)
- ZIO (Functional effects)
- fs2 (Functional streams)

---

*Built to demonstrate production-ready patterns for modern data engineering with Scala 3.* 🚀