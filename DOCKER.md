
# Docker Setup Guide

Complete Docker setup for running Scala 3 + Apache Spark with Hive Metastore.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Docker Compose Stack                  │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌──────────────┐      ┌──────────────────┐            │
│  │              │      │                  │            │
│  │  Spark App   │─────▶│ Hive Metastore  │            │
│  │ (Scala 3)    │      │   (Port 9083)   │            │
│  │              │      │                  │            │
│  └──────────────┘      └────────┬─────────┘            │
│                                 │                       │
│                                 ▼                       │
│                        ┌──────────────┐                │
│                        │  PostgreSQL  │                │
│                        │  (Port 5432) │                │
│                        └──────────────┘                │
│                                                          │
│  Optional Cluster Mode:                                 │
│  ┌──────────────┐      ┌──────────────┐                │
│  │ Spark Master │─────▶│ Spark Worker │                │
│  │ (Port 8080)  │      │ (Port 8081)  │                │
│  └──────────────┘      └──────────────┘                │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

## Components

### 1. PostgreSQL
- **Purpose**: Backend database for Hive Metastore
- **Port**: 5432
- **Credentials**: hive/hive
- **Database**: metastore

### 2. Hive Metastore
- **Purpose**: Metadata management for Spark SQL tables
- **Port**: 9083 (Thrift)
- **Image**: apache/hive:4.0.0

### 3. Spark Application
- **Purpose**: Your Scala 3 application
- **Port**: 4040 (Spark UI)
- **Base Image**: apache/spark:3.5.3-scala2.13-java17

### 4. Spark Cluster (Optional)
- **Spark Master**: Port 8080 (Web UI), 7077 (RPC)
- **Spark Worker**: Port 8081 (Web UI)

## Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- 4GB+ RAM available for Docker
- 10GB+ disk space

## Quick Start

### 1. Build and Start Services

```bash
# Start all services (local mode)
docker-compose up -d

# View logs
docker-compose logs -f spark-app

# Check service health
docker-compose ps
```

### 2. Access Services

- **Spark Application Logs**: `docker-compose logs -f spark-app`
- **Spark UI**: http://localhost:4040 (when app is running)
- **PostgreSQL**: `localhost:5432` (username: hive, password: hive)

### 3. Stop Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v
```

## Usage Modes

### Local Mode (Default)

Run Spark application with local executor:

```bash
docker-compose up spark-app
```

### Cluster Mode

Run with Spark master and workers:

```bash
# Start cluster components
docker-compose --profile cluster up -d

# Access Spark Master UI at http://localhost:8080
# Access Spark Worker UI at http://localhost:8081
```

## Building the Image

### Manual Build

```bash
# Build the application image
docker build -t scala3-spark:latest .

# Run specific version
docker-compose up spark-app
```

### Rebuild After Code Changes

```bash
# Rebuild and restart
docker-compose up -d --build spark-app
```

## Configuration

### Environment Variables

Edit `docker-compose.yml` to customize:

```yaml
environment:
  - SPARK_MASTER=local[*]              # Spark master URL
  - HIVE_METASTORE_URIS=thrift://...   # Metastore location
```

### Spark Configuration

Edit `docker/spark-defaults.conf`:

```properties
spark.sql.shuffle.partitions=4
spark.default.parallelism=4
```

### Hive Configuration

Edit `docker/hive-site.xml` for metastore settings.

## Working with Hive Tables

### Creating Tables

The application can create Hive-managed tables:

```scala
// In your Scala code
val peopleDF = spark.read
  .schema(Person.schema)
  .csv("/app/data/sample.csv")

// Save as Hive table
peopleDF.write
  .mode("overwrite")
  .saveAsTable("people")

// Query the table
spark.sql("SELECT * FROM people WHERE age >= 18").show()
```

### Accessing Tables from Shell

```bash
# Connect to Spark application container
docker exec -it scala3-spark-app /bin/bash

# Start Spark SQL shell
spark-sql

# Query tables
SELECT * FROM people;
```

## Volumes

Persistent data is stored in Docker volumes:

```bash
# List volumes
docker volume ls

# Inspect volume
docker volume inspect scala3-spark_postgres-data

# Remove volumes (WARNING: deletes data)
docker volume rm scala3-spark_postgres-data
```

### Volume Mappings

- `postgres-data`: PostgreSQL database
- `hive-warehouse`: Hive warehouse data
- `spark-warehouse`: Spark warehouse
- `./data:/app/data`: Application data (mounted from host)

## Troubleshooting

### Services Won't Start

```bash
# Check logs
docker-compose logs

# Check specific service
docker-compose logs hive-metastore

# Verify network
docker network ls
docker network inspect scala3-spark_spark-network
```

### Metastore Connection Issues

```bash
# Test metastore connectivity
docker exec -it scala3-spark-app nc -zv hive-metastore 9083

# Check metastore logs
docker-compose logs hive-metastore

# Restart metastore
docker-compose restart hive-metastore
```

### Out of Memory

Edit `docker-compose.yml`:

```yaml
spark-app:
  environment:
    - SPARK_DRIVER_MEMORY=2g
    - SPARK_EXECUTOR_MEMORY=2g
  deploy:
    resources:
      limits:
        memory: 4G
```

### Permission Issues

```bash
# Fix warehouse permissions
docker exec -it scala3-spark-app chmod 777 /app/spark-warehouse

# Fix data permissions
chmod -R 755 data/
```

### PostgreSQL Issues

```bash
# Connect to PostgreSQL
docker exec -it scala3-spark-postgres psql -U hive -d metastore

# List tables
\dt

# Check connections
SELECT * FROM pg_stat_activity;
```

## Development Workflow

### 1. Edit Code

```bash
# Edit src/main/scala/...
vim src/main/scala/com/example/spark3/Main.scala
```

### 2. Rebuild and Test

```bash
# Rebuild image
docker-compose build spark-app

# Run application
docker-compose up spark-app
```

### 3. Debug

```bash
# Run interactively
docker-compose run --rm spark-app /bin/bash

# Manual spark-submit
spark-submit \
  --class com.example.spark3.Main \
  --master local[*] \
  /app/app.jar
```

## Advanced Usage

### Custom Spark Submit

```bash
# Override default command
docker-compose run --rm spark-app \
  spark-submit \
  --class com.example.spark3.Main \
  --master local[4] \
  --conf spark.sql.shuffle.partitions=8 \
  /app/app.jar
```

### Connect to Cluster Mode

Edit application to connect to Spark cluster:

```yaml
# In docker-compose.yml
spark-app:
  environment:
    - SPARK_MASTER=spark://spark-master:7077
```

### Add Dependencies

Edit `build.sbt` and rebuild:

```scala
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-sql_2.13" % "3.5.3",
  "your.new" % "dependency" % "1.0.0"
)
```

Then:

```bash
docker-compose build spark-app
```

## Production Considerations

### Security

1. **Change default passwords**:
   ```yaml
   environment:
     POSTGRES_PASSWORD: strong_password_here
   ```

2. **Use secrets** (Docker Swarm/Kubernetes):
   ```yaml
   secrets:
     - postgres_password
   ```

3. **Enable SSL/TLS** for PostgreSQL and Metastore

### Performance

1. **Tune Spark resources**:
   ```yaml
   environment:
     - SPARK_DRIVER_MEMORY=4g
     - SPARK_EXECUTOR_MEMORY=4g
     - SPARK_EXECUTOR_CORES=2
   ```

2. **Scale workers**:
   ```bash
   docker-compose up -d --scale spark-worker=3
   ```

3. **Configure PostgreSQL** for better performance

### Monitoring

1. **Enable Spark History Server**
2. **Add Prometheus/Grafana** for metrics
3. **Configure logging** to external systems

## Clean Up

### Remove Everything

```bash
# Stop and remove containers, networks
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove images
docker rmi scala3-spark:latest
docker rmi apache/spark:3.5.3-scala2.13-java17-python3-ubuntu
docker rmi apache/hive:4.0.0
docker rmi postgres:15-alpine
```

### Prune Docker System

```bash
# Remove unused data
docker system prune -a

# Remove unused volumes
docker volume prune
```

## Additional Resources

- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Apache Spark on Docker](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Apache Hive Documentation](https://hive.apache.org/)
- [PostgreSQL Docker Image](https://hub.docker.com/_/postgres)

## Summary

This Docker setup provides:
- ✅ Fully containerized Scala 3 + Spark environment
- ✅ Hive Metastore for metadata management
- ✅ PostgreSQL backend for persistence
- ✅ Optional Spark cluster mode
- ✅ Volume mounts for data persistence
- ✅ Health checks and service dependencies
- ✅ Development-friendly workflow

Perfect for development, testing, and learning Spark with Scala 3!