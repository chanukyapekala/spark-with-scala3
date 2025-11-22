# Multi-stage Docker build for Scala 3 + Spark application

# Stage 1: Build the application
FROM sbtscala/scala-sbt:eclipse-temurin-17.0.15_6_1.11.7_3.3.7 AS builder

WORKDIR /app

# Copy build files
COPY build.sbt .
COPY project/build.properties project/
COPY project/plugins.sbt project/

# Download dependencies (cached layer)
RUN sbt update

# Copy source code
COPY src src/
COPY data data/

# Compile and create fat jar
RUN sbt clean assembly

# Stage 2: Runtime image
FROM apache/spark:3.5.7-scala2.12-java17-python3-ubuntu

USER root

# Install necessary tools
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    netcat \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Copy compiled application from builder
COPY --from=builder /app/target/scala-3.5.2/scala3-spark-assembly.jar /app/app.jar
COPY --from=builder /app/data /app/data

# Copy Hive configuration
COPY docker/hive-site.xml $SPARK_HOME/conf/hive-site.xml
COPY docker/spark-defaults.conf $SPARK_HOME/conf/spark-defaults.conf
RUN chmod 644 $SPARK_HOME/conf/hive-site.xml $SPARK_HOME/conf/spark-defaults.conf

# Set working directory
WORKDIR /app

# Create directory for Hive warehouse
RUN mkdir -p /app/spark-warehouse && \
    chmod 777 /app/spark-warehouse

# Java options for Spark with Java 17+
ENV SPARK_SUBMIT_OPTS="\
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.base/javax.security.auth=ALL-UNNAMED \
  -Djava.security.manager=allow"

# Entrypoint script
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod 755 /entrypoint.sh

USER spark

ENTRYPOINT ["/entrypoint.sh"]
CMD ["java", \
     "-cp", "/app/app.jar:/opt/spark/jars/*", \
     "--add-opens=java.base/java.lang=ALL-UNNAMED", \
     "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED", \
     "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
     "--add-opens=java.base/java.io=ALL-UNNAMED", \
     "--add-opens=java.base/java.net=ALL-UNNAMED", \
     "--add-opens=java.base/java.nio=ALL-UNNAMED", \
     "--add-opens=java.base/java.util=ALL-UNNAMED", \
     "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED", \
     "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED", \
     "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED", \
     "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED", \
     "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED", \
     "--add-opens=java.base/sun.security.action=ALL-UNNAMED", \
     "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED", \
     "--add-opens=java.base/javax.security.auth=ALL-UNNAMED", \
     "-Djava.security.manager=allow", \
     "com.example.spark3.Main"]
