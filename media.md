# Building a Modern Data Platform: Scala 3 + Kafka + Flink + Spark

## How This Project Started (Personal Story)

It's been a while since I've written serious Scala code. In the meantime, Scala 3.5+ rolled out with some genuinely exciting features — enums with actual parameters, opaque types for true type safety, extension methods that make APIs feel natural, and given/using for elegant dependency injection.

I wanted to actually **use** these features in a real project, not just read about them in tutorials.

The goal was simple but ambitious: **build a complete data engineering pipeline entirely in Scala**. No Python. No polyglot chaos. Just pure, type-safe Scala from event generation → real-time streaming → batch analytics.

But here's the plot twist: **Spark doesn't support Scala 3 yet**. Neither does Databricks (stuck on Scala 2.13, and likely will be for a while). Yet I still wanted to deploy JARs to Databricks and leverage the ecosystem.

So the real challenge became: *How do I use Scala 3's amazing features while still playing nicely with Spark?*

**Answer**: Clever module architecture. Scala 3 can read Scala 2.13 bytecode (forward compatible), but Scala 2 can't read Scala 3 bytecode (not backward compatible). So I:
- Used Scala 2.13 as a "bridge" layer that everyone can depend on
- Wrote event generation and streaming in Scala 3
- Connected them seamlessly despite the version gap
- Deployed everything as JARs to production (Databricks)

This project is what came out of that exploration — a working blueprint for teams wanting modern Scala without sacrificing the Spark ecosystem.

---

## Project Overview

This project demonstrates a **production-ready streaming data platform** that combines the best of modern data engineering:

- **Real-time streaming** with Apache Flink
- **Batch analytics** with Apache Spark
- **Modern language features** with Scala 3
- **Event-driven architecture** with Kafka
- **Workflow orchestration** with ZIO

It's the kind of system used by companies like Uber, Netflix, and LinkedIn to process billions of events daily.

## Architecture at a Glance

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
          ┌──────────┴──────────┐
          ▼                     ▼
┌──────────────────────┐  ┌──────────────────────────────┐
│  Flink Streaming     │  │  ZIO Orchestrator (Scala 3)  │
│  (Scala 3)           │  │  ├─ Generate 500 real events │
│  ├─ Consume from     │  │  ├─ Write Parquet data lake  │
│  │   Kafka (RT)      │  │  ├─ Read & validate records  │
│  ├─ Event-time       │  │  ├─ Compute batch analytics  │
│  │   processing      │  │  └─ Web dashboard @ :9090    │
│  └─ Write Parquet    │  └──────────────────────────────┘
└──────────┬───────────┘
           │ (Parquet files on disk/S3)
           │ Partitioned: dt=YYYY-MM-DD/hour=HH
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

## The 5 Modules

### 1. shared (Scala 2.13 — The Bridge)
The glue that holds everything together.

**Why Scala 2.13?**
- All modules can depend on it
- Scala 3 can read Scala 2.13 bytecode
- Scala 2.13 can read Scala 2.13 bytecode
- Scala 2 CANNOT read Scala 3 bytecode

**What it contains:**
```
├─ PersonEvent          (Kafka message schema)
├─ KafkaConfig          (Connection settings)
├─ Paths                (Local/S3 path configuration)
└─ PersonSchema         (Field definitions)
```

### 2. preprocessing (Scala 3 — Event Generation)
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

### 3. flink-streaming (Scala 3 — Stream Processing)
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

### 4. etl (Scala 2.13 — Batch Analytics)
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

### 5. orchestrator (Scala 3 + ZIO — Workflow Control)
Interactive dashboard that orchestrates the entire pipeline with **real data** — no mocks, no simulations.

**What it actually does:**
- Generates 500 real PersonEvent records per run
- Writes Parquet files to a partitioned data lake (`dt=YYYY-MM-DD/`)
- Reads back and validates every record from Parquet
- Computes batch analytics (city stats, age distribution, status breakdown) — mirroring what Spark does
- Tracks run history with cumulative totals across reruns

**Why ZIO?**
- Composable concurrent effects
- Type-safe error handling
- Non-blocking async execution of the DAG
- Clean state management for task metrics

**Features:**
- Web dashboard at http://localhost:9090
- Real-time execution logs and metrics
- Sample data tables populated from actual Parquet reads
- Run history showing data growth over time
- REST API for programmatic control

## Key Architectural Decisions

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
| shared | 2.13 | Bridge layer — all modules depend on it |
| preprocessing | 3.5.2 | Modern features for event generation |
| flink-streaming | 3.5.2 | Scala 3 syntax with Flink Java API |
| etl | 2.13 | Full Spark compatibility |
| orchestrator | 3.5.2 | Real data pipeline orchestration with ZIO |

**Why not all Scala 3?**
- Spark doesn't support Scala 3 yet (Databricks is stuck on 2.13)
- **Solution**: Use Scala 2.13 as a bridge layer, Scala 3 everywhere else

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

### Decision 5: Real Data Orchestration (No Mocks)
The orchestrator doesn't simulate the pipeline — it *runs* it. Every execution generates real PersonEvent records, writes real Parquet files, and computes real analytics. This means the dashboard always reflects the actual state of the data lake, not a canned demo.

## Code Quality Metrics

After extensive optimization:
- **Lines of Code**: 2,685 (lean, focused)
- **Scala Files**: 14 (minimal, essential)
- **Documentation**: 2 files (README.md + CLAUDE.md)
- **Build Tool**: Makefile with 20+ targets
- **Removed**: 7 unused orchestrator files, 8 duplicate docs, 72 LOC of dead code

## Getting Started (3 Options)

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

## Key Takeaways

### For Data Engineers
- Lambda architecture combines real-time speed with batch accuracy
- Kafka decouples data producers from consumers
- Flink provides exactly-once semantics (no data loss)
- Spark excels at analytical queries on the data lake

### For Scala Developers
- Scala 3 features (enums, opaque types) improve type safety
- ZIO provides elegant concurrent effect composition
- Multi-version Scala can work if architected carefully
- Functional programming shines in data pipelines

### For System Design
- Event-driven architecture scales horizontally
- Partitioning by time enables efficient data management
- Decoupling layers (Kafka) improves resilience
- A real-data orchestrator beats mock dashboards for credibility

## What Makes This Project Different

Most tutorials show:
- Single-language systems (not multi-version)
- Only real-time OR batch (not both)
- Toy examples (not production patterns)
- Mock dashboards with fake data

**This project shows:**
- How to manage Scala 2.13 + 3 together in one build
- Complete Lambda architecture implementation
- Real data flowing end-to-end: generate → Parquet → analytics → results
- Interactive orchestrator dashboard with live metrics from real data
- Modern Scala features in production context
- Clean codebase (14 files, 2,685 LOC) after aggressive optimization

## Production Deployment

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

## Learn More

**GitHub**: [chanukyapekala/spark-with-scala3](https://github.com/chanukyapekala/spark-with-scala3)

**Key Files**:
- `README.md` — Quick start guide
- `CLAUDE.md` — Technical deep dive
- `Makefile` — All development commands (`make help`)

**Technologies Used**:
- Apache Kafka (Event streaming)
- Apache Flink (Stream processing)
- Apache Spark (Batch analytics)
- Scala 3.5.2 & 2.13.12 (JVM language)
- ZIO (Functional effects & orchestration)
- fs2 (Functional streams)
- Parquet (Columnar data lake format)

---

*Built to demonstrate production-ready patterns for modern data engineering with Scala 3.*