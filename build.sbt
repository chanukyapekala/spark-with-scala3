name := "scala3-spark"

version := "0.1.0"

scalaVersion := "3.5.2"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.5.3").cross(CrossVersion.for3Use2_13),
  ("org.apache.spark" %% "spark-core" % "3.5.3").cross(CrossVersion.for3Use2_13)
)

// Spark uses reflection which needs this for Scala 3
scalacOptions ++= Seq(
  "-Xmax-inlines", "64"
)

fork := true

// Java 17+ compatibility with Spark
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
  "-Djava.security.manager=allow"
)

// Assembly settings for fat jar
import sbtassembly.AssemblyPlugin.autoImport._

assembly / assemblyJarName := "scala3-spark-assembly.jar"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => xs match {
    case "MANIFEST.MF" :: Nil => MergeStrategy.discard
    case "services" :: _ => MergeStrategy.concat
    case _ => MergeStrategy.discard
  }
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case x if x.endsWith(".proto") => MergeStrategy.rename
  case PathList("scala", xs @ _*) => MergeStrategy.first
  case x if x.contains("scala-library") => MergeStrategy.first
  case _ => MergeStrategy.first
}