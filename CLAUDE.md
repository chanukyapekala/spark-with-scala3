# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**spark-with-scala3** is a multi-module Scala project demonstrating how to use Scala 3 features alongside Spark (which requires Scala 2.13) in a single codebase. The architecture separates concerns by compilation target, communicating through the file system using Parquet files.

**Core Concept**: Preprocessing (Scala 3) writes Parquet → ETL (Scala 2.13 + Spark) reads Parquet. No direct code dependencies between Scala versions.

## Module Architecture

### Module Structure
- **shared/** (Scala 2.13) - Common schemas, paths, and configuration shared across modules
- **preprocessing/** (Scala 3.5.2) - Data preprocessing using modern Scala 3 features, writes Parquet
- **etl/** (Scala 2.13) - Spark operations for aggregations, joins, transformations

### Compilation Targets
- `shared`: Scala 2.13.12
- `preprocessing`: Scala 3.5.2 (demonstrates Scala 3 features: enums, opaque types, given/using, extension methods)
- `etl`: Scala 2.13.12 (uses Spark 3.5.3)

### Data Flow
```
data/raw/ (JSON/CSV)
  ↓
preprocessing (Scala 3) validates & transforms
  ↓
data/processed/ (Parquet)
  ↓
etl (Scala 2.13 + Spark) aggregates & analyzes
  ↓
data/output/ (Parquet/Delta)
```

## Common Build Commands

### Compile
```bash
# Compile all modules
sbt compile

# Compile specific module
sbt "preprocessing/compile"
sbt "etl/compile"
sbt "shared/compile"
```

### Run Applications
```bash
# Generate test data (creates data/raw/people.jsonl)
sbt "preprocessing/runMain generateTestData 1000"

# Run preprocessing pipeline (Scala 3)
sbt "preprocessing/run"

# Run ETL pipeline (Spark)
sbt "etl/run"

# Demo Scala 3 features
sbt "preprocessing/runMain demoScala3Features"
```

### Testing
```bash
# Test all modules
sbt test

# Test specific module
sbt "preprocessing/test"
sbt "etl/test"
```

### Build Assembly JARs
```bash
# Build preprocessing fat JAR
sbt "preprocessing/assembly"
# Output: preprocessing/target/scala-3.5.2/preprocessing-assembly.jar

# Build ETL fat JAR
sbt "etl/assembly"
# Output: etl/target/scala-2.13/etl-assembly.jar
```

### Clean Build
```bash
sbt clean
sbt clean compile
```

## Docker Commands

### Using Docker Compose (Recommended)
```bash
# Build and run entire pipeline
docker-compose up --build

# Run in background
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop everything
docker-compose down
```

### Build Individual Images
```bash
# Preprocessing image
docker build --build-arg MODULE=preprocessing --target preprocessing -t preprocessing:latest .

# ETL image
docker build --build-arg MODULE=etl --target etl -t etl:latest .
```

### Run Containers
```bash
# Preprocessing
docker run -v $(pwd)/data:/app/data preprocessing:latest

# ETL (with Spark UI on port 4040)
docker run -v $(pwd)/data:/app/data -p 4040:4040 etl:latest
```

## Key Architecture Patterns

### Cross-Module Communication
Modules communicate exclusively through the file system (Parquet files), not through code dependencies. The `shared` module (Scala 2.13) provides common schemas and paths that both other modules can read.

**Important**: Scala 3 can depend on Scala 2.13 bytecode, but NOT vice versa. This is why `shared` uses Scala 2.13 - both modules can depend on it.

### Path Configuration
Centralized in `shared/src/main/scala/shared/config/Paths.scala`. Supports both local development and S3/production paths via the `ENV` environment variable:
- `ENV=local` → uses `data/` directory
- `ENV=production` → uses S3 paths from `S3_BUCKET` env var

### Schema Definitions
Shared schemas in `shared/src/main/scala/shared/schemas/PersonSchema.scala` define:
- Field names as constants (prevents typos)
- Status values
- Validation constants
- Simple case classes for reference

**Note**: The preprocessing module defines its own Scala 3 models with advanced features (enums, opaque types) that wrap these shared schemas.

### Scala 3 Features Demonstrated
The `preprocessing` module showcases:
1. **Enums** - `PersonStatus` enum with parameters in `preprocessing/models/Person.scala:18`
2. **Opaque Types** - Zero-cost type wrappers like `Email`, `Age` for compile-time safety
3. **Extension Methods** - Adding methods to existing types without inheritance
4. **Given/Using** - Context parameters for cleaner implicits
5. **Union Types** - Expressing "either/or" types naturally
6. **Derives Clause** - Automatic derivation of type class instances
7. **Top-Level Definitions** - Functions and values without wrapping objects
8. **@main Annotation** - Entry points without boilerplate
9. **IOApp** - Cats Effect integration for functional IO

### Assembly JAR Configuration
Both modules use sbt-assembly with specific merge strategies for:
- META-INF/services → concat (for ServiceLoader)
- META-INF/** → discard (avoid signature issues)
- reference.conf → concat (merge Typesafe config)
- log4j2.properties → first (take first occurrence)

## Development Workflow

### Typical Development Cycle
1. Make code changes in appropriate module
2. Compile: `sbt "<module>/compile"`
3. Test: `sbt "<module>/test"`
4. Run locally: `sbt "<module>/run"`
5. For production: `sbt "<module>/assembly"` to build fat JAR

### Adding New Features
- **New data sources**: Add to `preprocessing` module
- **New transformations**: Add to `preprocessing` processors
- **New Spark operations**: Add to `etl/jobs/`
- **Shared config**: Add to `shared` module (use Scala 2.13 syntax)

### Common Gotchas
1. **Scala version mismatch**: Remember preprocessing is Scala 3, etl is Scala 2.13
2. **Dependency scope**: Spark deps are `Provided` in etl (available in cluster runtime)
3. **Hadoop client**: Also `Provided` scope - needed for compilation but available at runtime
4. **Path separators**: Use `shared.config.Paths` for all file paths to ensure consistency
5. **JVM forking**: Both modules have `fork := true` to run in separate JVMs

## Production Deployment

### Standalone JARs
```bash
# Run preprocessing
java -jar preprocessing/target/scala-3.5.2/preprocessing-assembly.jar

# Run ETL with spark-submit
spark-submit \
  --class etl.SparkETLPipeline \
  --master local[*] \
  etl/target/scala-2.13/etl-assembly.jar
```

### EMR Deployment
1. Upload assembly JARs to S3
2. Run preprocessing on EC2 or EMR (standard Java)
3. Submit ETL as Spark step with `spark-submit`
4. Set `ENV=production` and `S3_BUCKET` environment variables

### Docker Deployment
See Docker commands section above. The multi-stage Dockerfile builds minimal runtime images with only JRE (not JDK) for smaller footprint.

## Compiler Options

### Scala 3 (preprocessing)
- `-deprecation` - Show deprecation warnings
- `-feature` - Show feature warnings
- `-unchecked` - Show unchecked warnings

### Scala 2.13 (etl, shared)
- `-deprecation`
- `-feature`
- `-unchecked`
- `-Xlint` - Enable additional linting

### Java Options (for Spark)
- `--add-opens=java.base/sun.nio.ch=ALL-UNNAMED` - Java 17 compatibility for Spark

## File Structure Conventions

### Module Layout
```
<module>/
├── src/
│   ├── main/
│   │   └── scala/<module>/
│   │       ├── models/     (data models)
│   │       ├── processors/ (transformation logic)
│   │       └── jobs/       (main applications)
│   └── test/
│       └── scala/
└── target/                 (generated artifacts)
```

### Data Directory Layout
```
data/
├── raw/        (input: JSON, CSV, JSONL)
├── processed/  (intermediate: Parquet from preprocessing)
└── output/     (final: Parquet/Delta from ETL)
```

## Important Notes for Code Modifications

1. **Never mix Scala versions**: Keep preprocessing Scala 3, etl and shared Scala 2.13
2. **Shared module constraints**: Must use Scala 2.13 syntax only (no Scala 3 features)
3. **Assembly conflicts**: If adding new dependencies, update `assemblyMergeStrategy`
4. **Parquet schema evolution**: Maintain backward compatibility when changing Person schema
5. **Environment handling**: Always use `shared.config.Paths` for environment-aware paths
6. **Logging**: Both modules use Log4j2 - configuration in resources/log4j2.properties