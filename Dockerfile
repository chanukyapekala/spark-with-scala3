# Multi-Module Dockerfile for spark-with-scala3
# Builds either preprocessing or etl module based on MODULE build arg
#
# Usage:
#   docker build --build-arg MODULE=preprocessing -t preprocessing:latest .
#   docker build --build-arg MODULE=etl -t etl:latest .

# =============================================================================
# Stage 1: Build - Compile and create assembly JAR
# =============================================================================
FROM eclipse-temurin:17-jdk as builder

# Install sbt
RUN apt-get update && \
    apt-get install -y curl && \
    curl -L https://github.com/sbt/sbt/releases/download/v1.10.0/sbt-1.10.0.tgz | tar -xz -C /usr/local && \
    ln -s /usr/local/sbt/bin/sbt /usr/bin/sbt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project files
COPY build.sbt .
COPY project/ project/
COPY shared/ shared/
COPY preprocessing/ preprocessing/
COPY etl/ etl/

# Build argument to select which module to build
ARG MODULE=preprocessing
ENV MODULE=${MODULE}

# Compile and create assembly JAR
RUN sbt "${MODULE}/assembly"

# Move the assembly JAR to a predictable location
RUN if [ "$MODULE" = "preprocessing" ]; then \
      cp preprocessing/target/scala-3.5.2/preprocessing-assembly.jar /app/app.jar; \
    else \
      cp etl/target/scala-2.13/etl-assembly.jar /app/app.jar; \
    fi

# =============================================================================
# Stage 2: Runtime - Minimal JRE image
# =============================================================================
FROM eclipse-temurin:17-jre-jammy as runtime

WORKDIR /app

# Build argument (must be declared in each stage that uses it)
ARG MODULE=preprocessing
ENV MODULE=${MODULE}

# Copy the assembly JAR from builder stage
COPY --from=builder /app/app.jar /app/app.jar

# Create data directories
RUN mkdir -p /app/data/raw /app/data/processed /app/data/output

# Set the main class based on module
ENV MAIN_CLASS=${MODULE}.PreprocessingPipeline
RUN if [ "$MODULE" = "etl" ]; then \
      echo "etl.SparkETLPipeline" > /tmp/mainclass; \
    else \
      echo "preprocessing.PreprocessingPipeline" > /tmp/mainclass; \
    fi

# Labels
LABEL org.opencontainers.image.title="spark-with-scala3-${MODULE}"
LABEL org.opencontainers.image.description="Scala 3 + Spark multi-module architecture - ${MODULE} module"
LABEL org.opencontainers.image.version="0.1.0"

# Expose ports (if needed)
# EXPOSE 4040

# Default command - run the assembly JAR
# For preprocessing: runs PreprocessingPipeline
# For etl: runs SparkETLPipeline
CMD java -jar /app/app.jar

# =============================================================================
# Alternative: Specific module targets
# =============================================================================

# Preprocessing target
FROM runtime as preprocessing
ENV MODULE=preprocessing
LABEL module="preprocessing"

# ETL target
FROM runtime as etl
ENV MODULE=etl
LABEL module="etl"
# Install Spark dependencies if needed for local mode
RUN apt-get update && \
    apt-get install -y procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*