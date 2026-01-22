.PHONY: help setup orchestrator learn full-stack compile test clean docker-up docker-down

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m # No Color

help:
	@echo "$(BLUE)Scala 3 + Spark Data Platform$(NC)"
	@echo ""
	@echo "$(GREEN)Setup & Installation:$(NC)"
	@echo "  make setup              Check prerequisites & configure"
	@echo ""
	@echo "$(GREEN)Quick Start:$(NC)"
	@echo "  make orchestrator       Start interactive dashboard (5 min, no docker)"
	@echo "  make learn              Explore code & run examples (learning mode)"
	@echo "  make full-stack         Full pipeline with Kafka/Flink (requires docker)"
	@echo ""
	@echo "$(GREEN)Development:$(NC)"
	@echo "  make compile            Compile all modules"
	@echo "  make test               Run all tests"
	@echo "  make clean              Remove build artifacts"
	@echo ""
	@echo "$(GREEN)Docker:$(NC)"
	@echo "  make docker-up          Start Kafka, Zookeeper, Flink"
	@echo "  make docker-down        Stop all services"
	@echo "  make docker-logs        View Docker logs"
	@echo ""
	@echo "$(GREEN)Scala Modules:$(NC)"
	@echo "  make shared-compile     Compile shared (Scala 2.13)"
	@echo "  make preprocessing-run  Run preprocessing (Scala 3)"
	@echo "  make etl-run            Run Spark ETL (Scala 2.13)"
	@echo ""

# Setup - Check prerequisites
setup:
	@echo "$(BLUE)================================================$(NC)"
	@echo "$(BLUE)  Scala 3 + Spark Project Setup$(NC)"
	@echo "$(BLUE)================================================$(NC)"
	@echo ""
	@echo "$(GREEN)✓ Checking Java...$(NC)"
	@command -v java >/dev/null 2>&1 || { echo "❌ Java not found"; exit 1; }
	@java -version 2>&1 | head -1
	@echo ""
	@echo "$(GREEN)✓ Checking sbt...$(NC)"
	@command -v sbt >/dev/null 2>&1 || { echo "❌ sbt not found"; exit 1; }
	@sbt --version 2>&1 | head -1
	@echo ""
	@echo "$(GREEN)✓ Testing compilation...$(NC)"
	@sbt clean compile 2>&1 | grep -E "^\[success\]|error" | head -1
	@echo ""
	@if command -v docker >/dev/null 2>&1; then \
		echo "$(GREEN)✅ Docker found$(NC)"; \
		docker --version; \
		echo ""; \
		if command -v docker-compose >/dev/null 2>&1; then \
			echo "$(GREEN)✅ Docker Compose found$(NC)"; \
			docker-compose --version; \
		else \
			echo "$(YELLOW)⚠️  Docker Compose not found (optional)$(NC)"; \
		fi; \
	else \
		echo "$(YELLOW)⚠️  Docker not found (optional - only needed for Kafka/Flink)$(NC)"; \
	fi
	@echo ""
	@echo "$(BLUE)================================================$(NC)"
	@echo "$(GREEN)✅ Setup Complete!$(NC)"
	@echo "$(BLUE)================================================$(NC)"
	@echo ""
	@echo "Next steps:"
	@echo "  1. Start dashboard:    $(YELLOW)make orchestrator$(NC)"
	@echo "  2. Learn & explore:    $(YELLOW)make learn$(NC)"
	@echo "  3. Run full stack:     $(YELLOW)make full-stack$(NC)"
	@echo ""

# Quick start - Orchestrator dashboard
orchestrator:
	@echo "$(BLUE)================================================$(NC)"
	@echo "$(BLUE)  Starting Orchestrator Dashboard$(NC)"
	@echo "$(BLUE)================================================$(NC)"
	@echo ""
	@echo "$(YELLOW)📊 Compiling and starting dashboard...$(NC)"
	@echo ""
	@sbt "orchestrator/runMain orchestrator.OrchestratorDashboard" &
	@sleep 5
	@echo ""
	@echo "$(BLUE)================================================$(NC)"
	@echo "$(GREEN)✅ Dashboard Started!$(NC)"
	@echo "$(BLUE)================================================$(NC)"
	@echo ""
	@echo "$(YELLOW)🌐 Open your browser to:$(NC)"
	@echo "   http://localhost:9090"
	@echo ""
	@echo "$(YELLOW)📋 Available Endpoints:$(NC)"
	@echo "   GET  /                        - Dashboard UI"
	@echo "   GET  /api/tasks               - List all tasks"
	@echo "   GET  /api/metrics/<task-name> - Task metrics"
	@echo "   GET  /api/summary             - Execution summary"
	@echo "   POST /api/execute/<task-name> - Execute task"
	@echo ""
	@echo "$(YELLOW)Sample Tasks:$(NC)"
	@echo "   • PublishToKafkaTask"
	@echo "   • SubmitFlinkJobTask"
	@echo "   • WaitForParquetOutputTask"
	@echo "   • RunSparkETLTask"
	@echo ""

# Learning mode - Interactive exploration
learn:
	@echo "$(BLUE)================================================$(NC)"
	@echo "$(BLUE)  Learning Mode - Explore Scala 3 Features$(NC)"
	@echo "$(BLUE)================================================$(NC)"
	@echo ""
	@echo "Choose what you'd like to do:"
	@echo ""
	@echo "  1. Compile all modules"
	@echo "  2. Run Spark ETL locally"
	@echo "  3. View Preprocessing module (Scala 3)"
	@echo "  4. Run tests"
	@echo "  5. Start interactive Scala REPL"
	@echo ""
	@read -p "Enter choice (1-5): " choice; \
	case $$choice in \
		1) echo "$(YELLOW)📦 Compiling...$(NC)"; sbt compile;; \
		2) echo "$(YELLOW)⚡ Running Spark ETL...$(NC)"; sbt "etl/run";; \
		3) echo "$(YELLOW)📖 Opening Person.scala...$(NC)"; less preprocessing/src/main/scala/preprocessing/models/Person.scala;; \
		4) echo "$(YELLOW)🧪 Running tests...$(NC)"; sbt test;; \
		5) echo "$(YELLOW)🐚 Starting Scala REPL...$(NC)"; sbt "preprocessing/console";; \
		*) echo "Invalid choice";; \
	esac

# Full stack - Docker Compose
full-stack:
	@echo "$(BLUE)================================================$(NC)"
	@echo "$(BLUE)  Full Stack Setup (Kafka + Flink + Spark)$(NC)"
	@echo "$(BLUE)================================================$(NC)"
	@echo ""
	@command -v docker >/dev/null 2>&1 || { echo "❌ Docker not found"; exit 1; }
	@command -v docker-compose >/dev/null 2>&1 || { echo "❌ Docker Compose not found"; exit 1; }
	@echo "$(YELLOW)🐳 Starting infrastructure...$(NC)"
	@echo ""
	@docker-compose up -d zookeeper kafka kafka-ui
	@echo "$(GREEN)✅ Zookeeper and Kafka started$(NC)"
	@echo ""
	@echo "$(YELLOW)⏳ Waiting for Kafka (10 seconds)...$(NC)"
	@sleep 10
	@echo ""
	@echo "$(BLUE)================================================$(NC)"
	@echo "$(GREEN)✅ Full Stack Ready!$(NC)"
	@echo "$(BLUE)================================================$(NC)"
	@echo ""
	@echo "$(YELLOW)📊 Available UIs:$(NC)"
	@echo "   • Kafka UI: http://localhost:8080"
	@echo ""
	@echo "$(YELLOW)🚀 Next steps:$(NC)"
	@echo ""
	@echo "1. Start Flink (in another terminal):"
	@echo "   $(YELLOW)docker-compose up -d flink-jobmanager flink-taskmanager$(NC)"
	@echo "   # Flink UI: http://localhost:8081"
	@echo ""
	@echo "2. Start streaming generator (in another terminal):"
	@echo "   $(YELLOW)docker-compose up -d streaming-generator$(NC)"
	@echo ""
	@echo "3. Run Spark ETL (when ready):"
	@echo "   $(YELLOW)make etl-run$(NC)"
	@echo ""
	@echo "Or run everything at once:"
	@echo "   $(YELLOW)docker-compose up -d$(NC)"
	@echo ""

# Compile
compile:
	@echo "$(YELLOW)📦 Compiling all modules...$(NC)"
	@sbt compile

shared-compile:
	@echo "$(YELLOW)📦 Compiling shared (Scala 2.13)...$(NC)"
	@sbt "shared/compile"

# Test
test:
	@echo "$(YELLOW)🧪 Running tests...$(NC)"
	@sbt test

# Clean
clean:
	@echo "$(YELLOW)🧹 Cleaning build artifacts...$(NC)"
	@sbt clean
	@rm -rf data/streaming data/output

# Run modules
preprocessing-run:
	@echo "$(YELLOW)🚀 Running preprocessing (Scala 3)...$(NC)"
	@sbt "preprocessing/run"

etl-run:
	@echo "$(YELLOW)🚀 Running Spark ETL...$(NC)"
	@sbt "etl/run"

# Docker commands
docker-up:
	@echo "$(YELLOW)🐳 Starting Docker services...$(NC)"
	@docker-compose up -d
	@echo "$(GREEN)✅ Services started$(NC)"
	@echo ""
	@echo "Available UIs:"
	@echo "  • Kafka UI:  http://localhost:8080"
	@echo "  • Flink UI:  http://localhost:8081"

docker-down:
	@echo "$(YELLOW)🐳 Stopping Docker services...$(NC)"
	@docker-compose down
	@echo "$(GREEN)✅ Services stopped$(NC)"

docker-logs:
	@docker-compose logs -f

# Build assembly JARs
assembly:
	@echo "$(YELLOW)📦 Building assembly JARs...$(NC)"
	@sbt "preprocessing/assembly" "flinkStreaming/assembly" "etl/assembly"
	@echo "$(GREEN)✅ JARs built:$(NC)"
	@echo "   preprocessing/target/scala-3.5.2/preprocessing-assembly.jar"
	@echo "   flink-streaming/target/scala-3.5.2/flink-streaming-assembly.jar"
	@echo "   etl/target/scala-2.13/etl-assembly.jar"

# Version check
versions:
	@echo "$(YELLOW)Checking Scala versions...$(NC)"
	@echo "shared:              $$(sbt 'show shared/scalaVersion' 2>/dev/null | grep -oP '\d+\.\d+\.\d+')"
	@echo "preprocessing:       $$(sbt 'show preprocessing/scalaVersion' 2>/dev/null | grep -oP '\d+\.\d+\.\d+')"
	@echo "flink-streaming:     $$(sbt 'show flinkStreaming/scalaVersion' 2>/dev/null | grep -oP '\d+\.\d+\.\d+')"
	@echo "etl:                 $$(sbt 'show etl/scalaVersion' 2>/dev/null | grep -oP '\d+\.\d+\.\d+')"