# Scala 3 + Apache Spark Project

A modern, modular Scala 3 project demonstrating Apache Spark's DataFrame API with type-safe operations.

## Project Structure

```
scala3-spark/
├── src/main/scala/com/example/spark3/
│   ├── Main.scala                    # Application entry point
│   ├── models/
│   │   └── Person.scala              # Data models and schemas
│   ├── services/
│   │   └── DataProcessor.scala       # Business logic and data processing
│   └── utils/
│       └── SparkSessionBuilder.scala # Spark session configuration
├── data/
│   └── sample.csv                    # Sample data file
├── build.sbt                         # Build configuration
└── README.md                         # This file
```

## Features

- **Scala 3.5.2** - Latest Scala with modern syntax
- **Apache Spark 3.5.3** - Distributed data processing (using Scala 2.13 libraries)
- **Type-Safe Operations** - DataFrame to case class conversions
- **Modular Architecture** - Clean separation of concerns
- **Java 17+ Compatible** - Includes necessary JVM options

> **📖 Note**: This project uses Scala 3 with Spark libraries compiled for Scala 2.13. See [SCALA3_MIGRATION.md](SCALA3_MIGRATION.md) for a detailed explanation of why and how this works, including limitations and workarounds.

## Architecture

### Models (`models/`)
- **Person**: Case class representing person entities with schema definition and Row conversion

### Services (`services/`)
- **DataProcessor**: Encapsulates all data processing logic
  - Load data from CSV
  - Filter operations
  - Aggregations
  - Type-safe conversions

### Utils (`utils/`)
- **SparkSessionBuilder**: Configures and creates Spark sessions with sensible defaults

## Requirements

- Java 17+
- sbt 1.10.5+
- Scala 3.5.2 (managed by sbt)

## Running the Application

### Run the application
```bash
sbt run
```

### Compile only
```bash
sbt compile
```

### Package as JAR
```bash
sbt package
```

### Clean build artifacts
```bash
sbt clean
```

## Java 17+ Compatibility

This project includes JVM options in `build.sbt` to work around Java module system restrictions that affect Spark:

- Opens internal Java packages required by Spark
- Allows deprecated security manager APIs
- All configured automatically when using `sbt run`

## Example Output

The application demonstrates:
1. Loading CSV data with explicit schema
2. Filtering records by age
3. Aggregating data by city
4. Type-safe DataFrame → case class conversions
5. Formatted string output

## Data Format

Sample CSV format (`data/sample.csv`):
```csv
name,age,city
Alice,28,Seattle
Bob,35,Portland
...
```

## Extending the Project

To add new data processing logic:

1. **Add new models** in `models/` package
2. **Create services** in `services/` package for business logic
3. **Add utilities** in `utils/` package for reusable helpers
4. **Update Main.scala** to wire everything together

## Configuration

Key settings in `build.sbt`:
- Scala version: `3.5.2`
- Spark version: `3.5.3`
- Fork mode: enabled for proper JVM options
- Java module opens: configured for Spark compatibility

## License

Open source - feel free to use and modify.