// Multi-Module Build: Scala 3 Streaming Generator + Scala 2.12 Flink + Spark
// - Scala 3.5.2: streaming-generator (Kafka publisher with modern Scala features)
// - Scala 2.12: shared, flink-streaming, etl (maximum compatibility)

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"

// ============================================
// Root Project (Aggregates all modules)
// ============================================
lazy val root = (project in file("."))
  .aggregate(shared, preprocessing, flinkStreaming, etl)
  .settings(
    name := "spark-with-scala3",
    // Don't compile root, just aggregate
    publish / skip := true
  )

// ============================================
// Shared Module (Scala 2.13)
// - Common configuration
// - Schema definitions
// - Path constants
// - Kafka message schemas
// - Used by ALL modules (Scala 3 can read Scala 2.13)
// - Lowest common denominator for cross-version compatibility
// ============================================
lazy val shared = (project in file("shared"))
  .settings(
    name := "shared",
    scalaVersion := "2.13.12",

    libraryDependencies ++= Seq(
      // Configuration
      "com.typesafe" % "config" % "1.4.3",

      // Kafka (for message schemas and serialization)
      "org.apache.kafka" % "kafka-clients" % "3.6.1",

      // JSON (for Kafka message serialization)
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.3",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.3",

      // Hadoop client for HDFS/S3 paths (Provided - available in runtime)
      "org.apache.hadoop" % "hadoop-client" % "3.3.4" % Provided,

      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test
    )
  )

// ============================================
// Preprocessing Module (Scala 3.5.2)
// - THIS IS WHERE YOU LEARN SCALA 3!
// - Pure Scala 3 features
// - No Spark dependencies
// - Reads raw data, writes Parquet
// ============================================
lazy val preprocessing = (project in file("preprocessing"))
  .settings(
    name := "preprocessing",
    scalaVersion := "3.5.2",

    libraryDependencies ++= Seq(
      // JSON processing - full Scala 3 support
      "io.circe" %% "circe-core" % "0.14.6",
      "io.circe" %% "circe-generic" % "0.14.6",
      "io.circe" %% "circe-parser" % "0.14.6",

      // Kafka producer
      "org.apache.kafka" % "kafka-clients" % "3.6.1",

      // CSV processing
      "com.github.tototoshi" %% "scala-csv" % "2.0.0",

      // Parquet - pure Scala library!
      "com.github.mjakubowski84" %% "parquet4s-core" % "2.17.0",

      // Hadoop for Parquet (Provided in production, but needed for compilation)
      "org.apache.hadoop" % "hadoop-client" % "3.3.4" % Provided,

      // Functional streaming (fs2)
      "co.fs2" %% "fs2-core" % "3.10.2",
      "co.fs2" %% "fs2-io" % "3.10.2",

      // fs2-kafka for functional Kafka integration
      "com.github.fd4s" %% "fs2-kafka" % "3.5.1",

      // Cats Effect for IO
      "org.typelevel" %% "cats-effect" % "3.5.4",

      // Logging
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",

      // Testing
      "org.scalameta" %% "munit" % "1.0.0" % Test
    ),

    // Assembly settings for standalone JAR
    assembly / assemblyJarName := "preprocessing-assembly.jar",
    assembly / mainClass := Some("preprocessing.PreprocessingPipeline"),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case "log4j2.properties" => MergeStrategy.first
      case _ => MergeStrategy.first
    },

    // Scala 3 compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked"
    ),

    // Fork JVM for running
    fork := true
  )
  .dependsOn(shared)  // Can depend on Scala 2.13 shared module (Scala 3 can read 2.13)

// ============================================
// ETL Module (Scala 2.13)
// - Spark operations
// - Aggregations, joins, transformations
// - Reads Parquet from streaming data lake
// - Writes to Delta/Parquet/Tables
// - Scala 2.13 for better forward-compatibility
// ============================================
lazy val etl = (project in file("etl"))
  .settings(
    name := "etl",
    scalaVersion := "2.13.12",

    libraryDependencies ++= Seq(
      // Spark - Provided (available in cluster)
      "org.apache.spark" %% "spark-sql" % "3.5.3" % Provided,
      "org.apache.spark" %% "spark-core" % "3.5.3" % Provided,

      // Delta Lake for production tables
      "io.delta" %% "delta-spark" % "3.0.0",

      // Logging (same as preprocessing for consistency)
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",

      // Testing with Spark
      "org.scalatest" %% "scalatest" % "3.2.17" % Test
    ),

    // Assembly settings for Spark submit
    assembly / assemblyJarName := "etl-assembly.jar",
    assembly / mainClass := Some("etl.SparkETLPipeline"),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case "log4j2.properties" => MergeStrategy.first
      case _ => MergeStrategy.first
    },

    // Scala 2.13 compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlint"
    ),

    // Java options for Spark (Java 17 compatibility)
    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    ),

    // Fork JVM for running
    fork := true
  )
  .dependsOn(shared)  // Depends on Scala 2.13 shared module

// ============================================
// Flink Streaming Module (Scala 3.5.2)
// - Consumes events from Kafka
// - Windowing and stream processing
// - Writes Parquet to data lake
// - Real-time data ingestion layer
// - Uses Scala 3 features!
// ============================================
lazy val flinkStreaming = (project in file("flink-streaming"))
  .settings(
    name := "flink-streaming",
    scalaVersion := "3.5.2",

    libraryDependencies ++= Seq(
      // Flink with Scala 3 support (experimental in 1.18+)
      // Note: Using Java API for better Scala 3 compatibility
      "org.apache.flink" % "flink-streaming-java" % "1.18.1" % Provided,
      "org.apache.flink" % "flink-clients" % "1.18.1" % Provided,

      // Flink Kafka connector
      "org.apache.flink" % "flink-connector-kafka" % "3.0.2-1.18",

      // Flink Parquet support
      "org.apache.flink" % "flink-parquet" % "1.18.1",
      "org.apache.flink" % "flink-connector-files" % "1.18.1",

      // Parquet and Hadoop for file writing
      "org.apache.parquet" % "parquet-avro" % "1.13.1",
      "org.apache.hadoop" % "hadoop-client" % "3.3.4" % Provided,

      // Jackson for JSON (de)serialization
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.3",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.17.2",

      // Logging
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-core" % "2.20.0",
      "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.20.0",

      // Testing
      "org.scalameta" %% "munit" % "1.0.0" % Test,
      "org.apache.flink" % "flink-test-utils" % "1.18.1" % Test
    ),

    // Assembly settings for Flink submit
    assembly / assemblyJarName := "flink-streaming-assembly.jar",
    assembly / mainClass := Some("flink.StreamingJob"),

    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case "reference.conf" => MergeStrategy.concat
      case "log4j2.properties" => MergeStrategy.first
      case x if x.contains("module-info.class") => MergeStrategy.discard
      case _ => MergeStrategy.first
    },

    // Scala 3 compiler options
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked"
    ),

    // Java options for Flink
    javaOptions ++= Seq(
      "--add-opens=java.base/java.util=ALL-UNNAMED"
    ),

    // Fork JVM for running
    fork := true
  )
  .dependsOn(shared)  // Depends on Scala 2.13 shared module (Scala 3 can read 2.13)