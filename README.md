# spark-with-scala3

> **Multi-Module Architecture: Learn Scala 3 Features + Production Spark**

A learning-focused project demonstrating how to use **Scala 3.5.2** for data preprocessing and **Scala 2.13** for Spark operations in a single codebase.

## Why This Architecture?

### The Problem
- Scala 3 + Spark 3.x have compatibility issues (varargs, TypeTag, etc.)
- Can't use Scala 3 features with Spark directly
- Python is simpler, but you want type safety

### The Solution
**Separate concerns by module:**
- `preprocessing/` (Scala 3) - Learn modern Scala, write Parquet files
- `etl/` (Scala 2.13) - Use Spark without issues
- `shared/` (Scala 2.13) - Common configuration

**Communication: File System**
- Preprocessing writes Parquet → ETL reads Parquet
- No direct code dependencies between Scala versions
- Clean, production-ready pattern

## Project Structure

```
spark-with-scala3/
├── build.sbt                    # Multi-module configuration
├── project/
│   ├── build.properties
│   └── plugins.sbt
│
├── shared/                      # Scala 2.13
│   └── src/main/scala/shared/
│       ├── config/
│       │   └── Paths.scala      # Path configuration
│       └── schemas/
│           └── PersonSchema.scala  # Shared schemas
│
├── preprocessing/               # Scala 3.5.2 - LEARNING FOCUS
│   └── src/main/scala/preprocessing/
│       ├── models/
│       │   └── Person.scala     # Enums, opaque types, extensions
│       ├── processors/
│       │   └── DataProcessor.scala  # Given/using, inline
│       └── PreprocessingPipeline.scala  # @main, IOApp
│
├── etl/                         # Scala 2.13 - SPARK OPERATIONS
│   └── src/main/scala/etl/
│       ├── jobs/
│       └── SparkETLPipeline.scala  # Aggregations, joins
│
└── data/
    ├── raw/                     # Input data
    ├── processed/               # Parquet from preprocessing
    └── output/                  # Final results from ETL
```

## Quick Start

### 1. Generate Test Data

```bash
# Using sbt
sbt "preprocessing/runMain generateTestData 1000"

# Or specify count
sbt "preprocessing/runMain generateTestData 5000"
```

This creates `data/raw/people.jsonl` with test records.

### 2. Run Preprocessing (Scala 3)

```bash
sbt "preprocessing/run"
```

This:
- Reads JSON lines from `data/raw/`
- Validates using opaque types
- Transforms using extension methods
- Writes Parquet to `data/processed/`

### 3. Run ETL (Scala 2.13 + Spark)

```bash
sbt "etl/run"
```

This:
- Reads Parquet from `data/processed/`
- Performs aggregations (works perfectly!)
- Computes statistics by city
- Writes results to `data/output/`

### 4. See Scala 3 Features Demo

```bash
sbt "preprocessing/runMain demoScala3Features"
```

Demonstrates all Scala 3 features in action.

## What You're Learning

### Scala 3 Features (preprocessing module)

| # | Feature | File | Line |
|---|---------|------|------|
| 1 | **Enums** | `models/Person.scala` | 15 |
| 2 | **Union Types** | `models/Person.scala` | 45 |
| 3 | **Opaque Types** | `models/Person.scala` | 52 |
| 4 | **Derives Clause** | `models/Person.scala` | 116 |
| 5 | **Extension Methods** | `models/Person.scala` | 172 |
| 6 | **Top-Level Definitions** | `models/Person.scala` | 200 |
| 7 | **Given/Using** | `processors/DataProcessor.scala` | 15 |
| 8 | **Given Instances** | `processors/DataProcessor.scala` | 25 |
| 9 | **Context Functions** | `processors/DataProcessor.scala` | 52 |
| 10 | **Inline Functions** | `processors/DataProcessor.scala` | 85 |
| 11 | **IOApp** | `PreprocessingPipeline.scala` | 15 |
| 12 | **@main Annotation** | `PreprocessingPipeline.scala` | 48 |

### Spark Operations (etl module)

- Multiple aggregations (no varargs issues!)
- Window functions
- Complex transformations
- Ready for Databricks/EMR deployment

## Architecture Benefits

### ✅ Learning
- **Focus on Scala 3** without fighting Spark compatibility
- **See real patterns** used in production
- **Understand boundaries** between local and distributed processing

### ✅ Technical
- **No compatibility issues** - each module uses native tooling
- **Single codebase** - easy to manage
- **Clean separation** - clear responsibilities

### ✅ Production
- **Cost optimization** - cheap preprocessing, expensive Spark only when needed
- **Independent scaling** - run modules on different infrastructure
- **Flexible deployment** - EMR, Databricks, local, Docker

## Development Workflow

### Build Everything

```bash
sbt compile
```

### Test Individual Modules

```bash
# Test preprocessing
sbt "preprocessing/test"

# Test ETL
sbt "etl/test"
```

### Create Assembly JARs

```bash
# Preprocessing assembly
sbt "preprocessing/assembly"
# Output: preprocessing/target/scala-3.5.2/preprocessing-assembly.jar

# ETL assembly
sbt "etl/assembly"
# Output: etl/target/scala-2.13/etl-assembly.jar
```

### Run Standalone

```bash
# Preprocessing
java -jar preprocessing/target/scala-3.5.2/preprocessing-assembly.jar

# ETL (requires Spark)
spark-submit \
  --class etl.SparkETLPipeline \
  --master local[*] \
  etl/target/scala-2.13/etl-assembly.jar
```

## Deployment

### Local Development
```bash
# Run everything locally
sbt "preprocessing/run"
sbt "etl/run"
```

### EMR Deployment

```bash
# Step 1: Upload JARs to S3
aws s3 cp preprocessing/target/scala-3.5.2/preprocessing-assembly.jar \
  s3://your-bucket/jars/

aws s3 cp etl/target/scala-2.13/etl-assembly.jar \
  s3://your-bucket/jars/

# Step 2: Run preprocessing (can run on EC2 or EMR)
java -jar preprocessing-assembly.jar

# Step 3: Run ETL on EMR
aws emr add-steps \
  --cluster-id j-XXXXX \
  --steps Type=SPARK,Name="ETL",\
Args=[--class,etl.SparkETLPipeline,s3://your-bucket/jars/etl-assembly.jar]
```

### Docker Deployment

This project includes a **single multi-stage Dockerfile** that builds both modules.

#### Quick Start with Docker Compose

```bash
# Build and run the entire pipeline
docker-compose up --build

# Run in background
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop everything
docker-compose down
```

#### Build Individual Images

**Preprocessing (Scala 3):**
```bash
docker build \
  --build-arg MODULE=preprocessing \
  --target preprocessing \
  -t preprocessing:latest \
  .
```

**ETL (Scala 2.13 + Spark):**
```bash
docker build \
  --build-arg MODULE=etl \
  --target etl \
  -t etl:latest \
  .
```

#### Running Containers

**Preprocessing:**
```bash
docker run \
  -v $(pwd)/data:/app/data \
  preprocessing:latest
```

**ETL:**
```bash
docker run \
  -v $(pwd)/data:/app/data \
  -p 4040:4040 \
  etl:latest

# Access Spark UI at http://localhost:4040
```

#### Complete Pipeline Example

```bash
# 1. Generate test data (local)
sbt "preprocessing/runMain generateTestData 1000"

# 2. Run preprocessing (Docker)
docker run -v $(pwd)/data:/app/data preprocessing:latest

# 3. Run ETL (Docker)
docker run -v $(pwd)/data:/app/data -p 4040:4040 etl:latest
```

#### Production Deployment

**AWS ECR:**
```bash
# Login to ECR
aws ecr get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin \
  123456789.dkr.ecr.us-east-1.amazonaws.com

# Build and push
docker build --build-arg MODULE=preprocessing \
  -t 123456789.dkr.ecr.us-east-1.amazonaws.com/preprocessing:latest .
docker push 123456789.dkr.ecr.us-east-1.amazonaws.com/preprocessing:latest

docker build --build-arg MODULE=etl \
  -t 123456789.dkr.ecr.us-east-1.amazonaws.com/etl:latest .
docker push 123456789.dkr.ecr.us-east-1.amazonaws.com/etl:latest
```

**Docker Compose Workflow:**
```bash
# Build both images
docker-compose build

# Run preprocessing only
docker-compose up preprocessing

# Run full pipeline
docker-compose up

# Stop and cleanup
docker-compose down
```

**Environment Variables:**

| Module | Variable | Default | Description |
|--------|----------|---------|-------------|
| preprocessing | `ENV` | `local` | Environment (local/production) |
| preprocessing | `S3_BUCKET` | - | S3 bucket for production |
| etl | `ENV` | `local` | Environment |
| etl | `SPARK_MASTER` | `local[*]` | Spark master URL |

**Volume Mounts:**
```bash
# Mount data directory
docker run -v $(pwd)/data:/app/data preprocessing:latest

# Data structure:
# data/
# ├── raw/        # Input data
# ├── processed/  # Parquet from preprocessing
# └── output/     # Results from ETL
```

**Advanced Docker Topics:**
- Multi-stage build optimizations
- Kubernetes deployment with Jobs
- ECS task definitions
- Security scanning with Trivy
- CI/CD with GitHub Actions

## Module Communication

```
┌─────────────────────────────────┐
│  preprocessing (Scala 3.5.2)   │
│                                 │
│  - Read JSON/CSV                │
│  - Validate (opaque types)      │
│  - Transform (extensions)       │
│  - Write Parquet                │
└────────────┬────────────────────┘
             │
             │ File System
             │ (Parquet files)
             │
             ▼
┌─────────────────────────────────┐
│  etl (Scala 2.13 + Spark)      │
│                                 │
│  - Read Parquet                 │
│  - Aggregate (works perfectly!) │
│  - Join, transform              │
│  - Write Delta/Parquet          │
└─────────────────────────────────┘
```

**Key Point:** No code dependency between modules, only data dependency.

## Scala 3 vs Scala 2 Comparison

### In preprocessing/ (Scala 3)

```scala
// Enums with parameters
enum PersonStatus:
  case Active
  case Suspended(reason: String)

// Opaque types (zero-cost)
opaque type Email = String

// Extension methods
extension (p: Person)
  def isAdult: Boolean = p.age >= 18

// Given/using (cleaner implicits)
def process[A](data: A)(using processor: Processor[A]): Unit
```

### In etl/ (Scala 2.13)

```scala
// Standard Spark operations
df.groupBy($"city")
  .agg(
    avg($"age"),      // Works perfectly!
    count($"*"),
    stddev($"age")
  )

// No compatibility issues
val people: Dataset[Person] = df.as[Person]  // If you had encoders
```

## When to Use This Pattern

### ✅ Use This When:
- You want to learn Scala 3
- You need type safety in ETL
- You have significant preprocessing logic
- Cost optimization matters
- You want production-ready patterns

### ❌ Don't Use This When:
- Everything needs distributed compute
- Team only knows Python
- Simple read-aggregate-write pipeline
- You prefer simplicity over type safety

## Comparison with Python

### Python + PySpark (Simple)
```python
# Single language, simple syntax
df = spark.read.json("data.json")
stats = df.groupBy("city").agg(avg("age"), count("*"))
stats.show()
```

### This Project (Type-Safe)
```scala
// Scala 3: Type-safe validation
val person: Validated[Person] = Person.create(name, email, age, city)

// Scala 2.13: Powerful Spark operations
val stats = df.groupBy($"city").agg(avg($"age"), count($"*"))
```

**Trade-off:** More complexity, but compile-time safety and better refactoring.

## Learning Path

### Week 1: Scala 3 Features
1. Run `demoScala3Features` - see all features
2. Read `preprocessing/models/Person.scala` - understand enums, opaque types
3. Modify models - add new fields, validation rules
4. Generate data - see validation in action

### Week 2: Processing Pipeline
1. Generate test data with different sizes
2. Run preprocessing - watch validation
3. Read Parquet files - understand format
4. Add new processors - CSV, different schemas

### Week 3: Spark Operations
1. Run ETL pipeline
2. Modify aggregations
3. Add new transformations
4. Try window functions

### Week 4: Deployment
1. Build assembly JARs
2. Run standalone
3. Deploy to EMR (if available)
4. Set up CI/CD pipeline

## Troubleshooting

### Module doesn't compile
```bash
# Clean and rebuild
sbt clean
sbt "preprocessing/compile"
sbt "etl/compile"
```

### Parquet file not found
```bash
# Make sure you run preprocessing first
sbt "preprocessing/run"

# Then run ETL
sbt "etl/run"
```

### Out of memory
```bash
# Increase JVM heap
export SBT_OPTS="-Xmx4G"
sbt "etl/run"
```

## Next Steps

1. **Add more processors** - Avro, ORC, Delta Lake
2. **Add streaming** - Use fs2-kafka for real-time ingestion
3. **Add tests** - Comprehensive unit and integration tests
4. **Add CI/CD** - GitHub Actions, deployment automation
5. **Add monitoring** - Metrics, logging, alerting
6. **Scale up** - Deploy to production cluster

## Resources

- [Scala 3 Book](https://docs.scala-lang.org/scala3/book/introduction.html)
- [Spark Scala API](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html)
- [Cats Effect](https://typelevel.org/cats-effect/)
- [fs2 Streaming](https://fs2.io/)
- [Parquet4s](https://github.com/mjakubowski84/parquet4s)

## Contributing

This is a learning project! Feel free to:
- Add more Scala 3 features
- Improve examples
- Add documentation
- Share your learnings

## License

MIT License - feel free to use for learning and production!

---

**Happy Learning! 🎓**

Built with ❤️ to demonstrate Scala 3 + Spark best practices.