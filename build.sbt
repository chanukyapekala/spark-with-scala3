// Multi-Module Build: Scala 3 Preprocessing + Scala 2.13 Spark ETL
// This allows us to use Scala 3 features where they work (preprocessing)
// and Scala 2.13 where needed (Spark operations)

ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "com.example"

// ============================================
// Root Project (Aggregates all modules)
// ============================================
lazy val root = (project in file("."))
  .aggregate(shared, preprocessing, etl)
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
// - Both other modules depend on this
// ============================================
lazy val shared = (project in file("shared"))
  .settings(
    name := "shared",
    scalaVersion := "2.13.12",

    libraryDependencies ++= Seq(
      // Configuration
      "com.typesafe" % "config" % "1.4.3",

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

      // CSV processing
      "com.github.tototoshi" %% "scala-csv" % "2.0.0",

      // Parquet - pure Scala library!
      "com.github.mjakubowski84" %% "parquet4s-core" % "2.17.0",

      // Hadoop for Parquet (Provided in production, but needed for compilation)
      "org.apache.hadoop" % "hadoop-client" % "3.3.4" % Provided,

      // Functional streaming (fs2)
      "co.fs2" %% "fs2-core" % "3.10.2",
      "co.fs2" %% "fs2-io" % "3.10.2",

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
  .dependsOn(shared)  // Can depend on Scala 2.13 shared module

// ============================================
// ETL Module (Scala 2.13)
// - Spark operations
// - Aggregations, joins, transformations
// - Reads Parquet from preprocessing
// - Writes to Delta/Parquet/Tables
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