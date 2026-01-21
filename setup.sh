#!/bin/bash
set -e

echo "================================================"
echo "  Scala 3 + Spark Project Setup"
echo "================================================"
echo ""

# Check Java
echo "✓ Checking Java..."
if ! command -v java &> /dev/null; then
    echo "❌ Java not found. Please install Java 11+"
    exit 1
fi
JAVA_VERSION=$(java -version 2>&1 | grep -oP 'version "\K[^"]+')
echo "  Found Java: $JAVA_VERSION"
echo ""

# Check sbt
echo "✓ Checking sbt..."
if ! command -v sbt &> /dev/null; then
    echo "⚠️  sbt not found. Installing..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install sbt
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        curl https://bintray.com/sbt/rpm/rpm | sudo tee /etc/yum.repos.d/bintray-sbt-rpm.repo
        sudo yum install -y sbt
    else
        echo "❌ Please install sbt manually from https://www.scala-sbt.org/download.html"
        exit 1
    fi
else
    SBT_VERSION=$(sbt --version 2>&1 | grep -oP '\d+\.\d+\.\d+')
    echo "  Found sbt: $SBT_VERSION"
fi
echo ""

# Check Docker (optional)
echo "✓ Checking Docker (optional)..."
if command -v docker &> /dev/null; then
    DOCKER_VERSION=$(docker --version | grep -oP 'Docker version \K[^,]+')
    echo "  ✅ Docker found: $DOCKER_VERSION"
    DOCKER_AVAILABLE=true

    if command -v docker-compose &> /dev/null; then
        DC_VERSION=$(docker-compose --version | grep -oP '\d+\.\d+\.\d+')
        echo "  ✅ Docker Compose found: $DC_VERSION"
    else
        echo "  ⚠️  Docker Compose not found (needed for full stack)"
    fi
else
    echo "  ⚠️  Docker not found (optional - only needed for Kafka/Flink)"
    DOCKER_AVAILABLE=false
fi
echo ""

# Test compilation
echo "✓ Testing compilation..."
sbt clean compile 2>&1 | grep -E "^\[success\]|error" | head -1
echo ""

# Offer Docker setup
if [ "$DOCKER_AVAILABLE" = true ]; then
    echo "🐳 Docker is available!"
    echo ""
    read -p "Would you like to start Kafka & Zookeeper now? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker-compose up -d zookeeper kafka
        echo "✅ Kafka & Zookeeper started"
        sleep 3
    fi
else
    echo "💡 Tip: To run the full streaming pipeline, install Docker:"
    echo "   https://docs.docker.com/get-docker/"
fi
echo ""

echo "================================================"
echo "  ✅ Setup Complete!"
echo "================================================"
echo ""
echo "Next steps:"
echo ""
echo "1️⃣  Run just the orchestrator dashboard:"
echo "   ./run-orchestrator.sh"
echo ""
echo "2️⃣  Explore code & run Spark ETL:"
echo "   sbt 'preprocessing/compile'"
echo "   sbt 'etl/run'"
echo ""
echo "3️⃣  Run full stack (requires Docker):"
echo "   ./run-full-stack.sh"
echo ""