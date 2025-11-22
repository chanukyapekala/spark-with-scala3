#!/bin/bash
set -e

echo "================================================"
echo "Scala 3 + Spark + Hive Metastore Application"
echo "================================================"

# Wait for Hive Metastore to be ready
echo "Waiting for Hive Metastore to be available..."
until nc -z hive-metastore 9083; do
  echo "Hive Metastore is unavailable - sleeping"
  sleep 2
done
echo "Hive Metastore is up - proceeding"

# Wait a bit more to ensure metastore is fully initialized
sleep 5

echo "Starting Spark application..."
echo "------------------------------------------------"

# Execute the command passed to the container
exec "$@"