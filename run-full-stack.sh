#!/bin/bash
set -e

echo "================================================"
echo "  Full Stack Setup (Kafka + Flink + Spark)"
echo "================================================"
echo ""

# Check Docker
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is required for full stack"
    echo "   Install from: https://docs.docker.com/get-docker/"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is required"
    echo "   Install from: https://docs.docker.com/compose/install/"
    exit 1
fi

echo "🐳 Starting infrastructure with Docker Compose..."
echo ""

# Start services
docker-compose up -d zookeeper kafka kafka-ui

echo "✅ Zookeeper and Kafka started"
echo ""

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
sleep 10

echo ""
echo "================================================"
echo "  ✅ Full Stack Ready!"
echo "================================================"
echo ""
echo "📊 Available UIs:"
echo "   • Kafka UI: http://localhost:8080"
echo ""
echo "🚀 Next steps:"
echo ""
echo "1. Start Flink (in another terminal):"
echo "   docker-compose up -d flink-jobmanager flink-taskmanager"
echo "   # Flink UI: http://localhost:8081"
echo ""
echo "2. Start streaming generator (in another terminal):"
echo "   docker-compose up -d streaming-generator"
echo ""
echo "3. Run Spark ETL (when you're ready):"
echo "   sbt 'etl/run'"
echo ""
echo "Or run everything at once:"
echo "   docker-compose up -d"
echo ""
echo "View logs:"
echo "   docker-compose logs -f [service-name]"
echo ""
echo "Stop everything:"
echo "   docker-compose down"
echo ""