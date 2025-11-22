#!/bin/bash
# Test script for Docker setup

set -e

echo "================================================"
echo "  Scala 3 + Spark + Hive Metastore Docker Test"
echo "================================================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

# Check if Docker is running
echo "1. Checking Docker..."
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker Desktop."
    exit 1
fi
print_status "Docker is running"

# Optional: Clean previous runs
read -p "Clean previous Docker containers/volumes? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "2. Cleaning up..."
    docker-compose down -v 2>/dev/null || true
    print_status "Cleaned up previous containers"
else
    echo "2. Skipping cleanup"
fi

# Start services
echo "3. Starting services (this may take 5-10 minutes on first run)..."
docker-compose up -d

# Wait for services
echo "4. Waiting for services to be healthy..."
echo "   - PostgreSQL (15 sec)"
sleep 15

echo "   - Hive Metastore (60 sec)"
sleep 45

echo "   - Building Spark application (5-10 min first time)"
echo "     You can monitor progress in another terminal with:"
echo "     docker-compose logs -f spark-app"
echo ""

# Check service status
echo "5. Service Status:"
docker-compose ps

# Wait for app to complete
echo ""
echo "6. Waiting for Spark application to complete..."
timeout 600 docker-compose logs -f spark-app 2>/dev/null || true

# Final status
echo ""
echo "================================================"
echo "  Test Complete!"
echo "================================================"
echo ""
echo "Service URLs:"
echo "  - Spark UI (when running): http://localhost:4040"
echo "  - PostgreSQL: localhost:5432 (user: hive, pass: hive)"
echo ""
echo "Useful commands:"
echo "  - View logs:     docker-compose logs -f spark-app"
echo "  - Check status:  docker-compose ps"
echo "  - Stop services: docker-compose down"
echo "  - Clean all:     docker-compose down -v"
echo ""