# Pipeline Orchestrator Tools

Complete toolkit for running, visualizing, and monitoring your data pipeline.

## Available Tools

```
┌─────────────────────────────────────────────────────────────────┐
│         ZIO-Based Pipeline Orchestrator Toolkit                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1️⃣  PipelineOrchestrator    - Execute the pipeline             │
│      Entry: orchestrator.OrchestratorMain                      │
│                                                                 │
│  2️⃣  PipelineDAG            - Visualize DAG structure           │
│      Entry: orchestrator.PipelineDAGMain                       │
│                                                                 │
│  3️⃣  WebUI                  - Interactive web dashboard         │
│      Entry: orchestrator.WebUIMain                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

## Quick Reference

### 1️⃣ Run the Pipeline

```bash
# Default: 100 events
sbt "orchestrator/runMain orchestrator.OrchestratorMain"

# Custom: 500 events
sbt "orchestrator/runMain orchestrator.OrchestratorMain 500"
```

**Output**: Executes complete pipeline (Kafka → Flink → ETL)

**Stages**:
1. Publish 100 events to Kafka (~8s)
2. Submit Flink job (~33s)
3. Wait for Parquet output (~30s)
4. Run Spark ETL (~55s)

**Total Time**: ~2 minutes

---

### 2️⃣ Visualize the DAG

```bash
# Show ASCII DAG
sbt "orchestrator/runMain orchestrator.PipelineDAGMain dag"

# Show execution timeline
sbt "orchestrator/runMain orchestrator.PipelineDAGMain timeline"

# Show execution trace with timing
sbt "orchestrator/runMain orchestrator.PipelineDAGMain trace"

# Show critical path analysis
sbt "orchestrator/runMain orchestrator.PipelineDAGMain critical"

# Show all visualizations
sbt "orchestrator/runMain orchestrator.PipelineDAGMain all"
```

**Available Commands**:
- `dag` - Pipeline structure
- `dependencies` - Dependency graph
- `timeline` - Chronological view
- `trace` - Timing breakdown
- `critical` - Bottleneck analysis
- `stages` - Stage details
- `dry-run` - Preview without execution
- `all` - Complete analysis

**Output**: ASCII art visualizations and analysis

---

### 3️⃣ Web Dashboard

```bash
# Start the web server
sbt "orchestrator/runMain orchestrator.WebUIMain"

# Then open browser
open http://localhost:8888/
```

**Features**:
- Interactive dashboard with statistics
- View switching buttons (DAG, Timeline, Stages, Trace, Critical)
- REST API endpoints for all visualizations
- Beautiful gradient UI with animations

**API Endpoints**:
- `GET /` - Dashboard HTML
- `GET /api/dag` - DAG as JSON
- `GET /api/timeline` - Timeline as JSON
- `GET /api/stages` - Stages as JSON
- `GET /api/trace` - Execution trace as JSON
- `GET /api/critical` - Critical path as JSON
- `GET /health` - Health check

---

## Complete Workflow

### Setup

```bash
# Terminal 1: Start infrastructure
docker-compose up -d zookeeper kafka flink-jobmanager flink-taskmanager

# Terminal 2: Start web UI (optional)
sbt "orchestrator/runMain orchestrator.WebUIMain"
# Open http://localhost:8888/
```

### Execution

```bash
# Terminal 3: View DAG before running
sbt "orchestrator/runMain orchestrator.PipelineDAGMain dag"

# Terminal 4: Run the pipeline
sbt "orchestrator/runMain orchestrator.OrchestratorMain 100"
```

### Monitoring

```bash
# Terminal 5: Monitor Kafka
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Terminal 6: Check output
ls -R data/streaming/people/
```

---

## Tool Comparison

| Feature | PipelineOrchestrator | PipelineDAG | WebUI |
|---------|-------------------|-----------|-------|
| Execute pipeline | ✅ Yes | ❌ No | ❌ No |
| View DAG | ❌ No | ✅ Yes | ✅ Yes |
| Web interface | ❌ No | ❌ No | ✅ Yes |
| CLI interface | ✅ Yes | ✅ Yes | ❌ No |
| REST API | ❌ No | ❌ No | ✅ Yes |
| Timing analysis | ❌ No | ✅ Yes | ✅ Yes |
| Pretty output | ✅ Yes | ✅ Yes | ✅ Yes |
| No dependencies | ✅ Yes | ✅ Yes | ✅ Yes |
| JSON output | ❌ No | ❌ No | ✅ Yes |

---

## Real-World Usage Scenarios

### Scenario 1: Quick Execution
```bash
# Just run it
sbt "orchestrator/runMain orchestrator.OrchestratorMain"
```

### Scenario 2: Understanding the Pipeline
```bash
# Understand the structure first
sbt "orchestrator/runMain orchestrator.PipelineDAGMain all"

# Then run it
sbt "orchestrator/runMain orchestrator.OrchestratorMain"
```

### Scenario 3: Team Visualization
```bash
# Start web UI for team to see
sbt "orchestrator/runMain orchestrator.WebUIMain"

# Share URL: http://localhost:8888/
# Team members open in browser to monitor
```

### Scenario 4: CI/CD Pipeline Status
```bash
# Health check
curl -f http://localhost:8888/health

# Get DAG as JSON for logging
curl http://localhost:8888/api/dag | jq '.data' >> pipeline.log

# Run pipeline
sbt "orchestrator/runMain orchestrator.OrchestratorMain"
```

### Scenario 5: Performance Analysis
```bash
# Understand timing constraints
sbt "orchestrator/runMain orchestrator.PipelineDAGMain trace"

# Critical path bottleneck
sbt "orchestrator/runMain orchestrator.PipelineDAGMain critical"

# Optimize based on findings
```

---

## Integration Examples

### with Docker Compose
```yaml
services:
  pipeline-orchestrator:
    build: .
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9093
    command: sbt "orchestrator/runMain orchestrator.OrchestratorMain"

  pipeline-web:
    build: .
    ports:
      - "8888:8888"
    command: sbt "orchestrator/runMain orchestrator.WebUIMain"
```

### with Kubernetes
```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pipeline-orchestrator
spec:
  template:
    spec:
      containers:
      - name: orchestrator
        image: scala3-spark:latest
        command: ["sbt", "orchestrator/runMain", "orchestrator.OrchestratorMain", "100"]
```

### with Cron
```bash
# Run pipeline every hour
0 * * * * cd /path/to/scala3-spark && sbt "orchestrator/runMain orchestrator.OrchestratorMain" >> /tmp/pipeline.log 2>&1
```

### with Systemd
```ini
[Unit]
Description=Pipeline Orchestrator
After=kafka.service flink.service

[Service]
Type=simple
WorkingDirectory=/path/to/scala3-spark
ExecStart=/usr/bin/sbt "orchestrator/runMain orchestrator.OrchestratorMain"
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│  Orchestrator Toolkit (Scala 3 + ZIO)                           │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Shared Infrastructure                                   │   │
│  │ ├─ PipelineDAG (sealed trait stages)                   │   │
│  │ ├─ PipelineDAGEffects (ZIO effects)                    │   │
│  │ └─ DAG visualization functions                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────┐ │
│  │ Orchestrator     │  │ DAG CLI          │  │ Web UI       │ │
│  ├──────────────────┤  ├──────────────────┤  ├──────────────┤ │
│  │ • Execute stages │  │ • View DAG       │  │ • Dashboard  │ │
│  │ • Error handling │  │ • Timeline       │  │ • REST API   │ │
│  │ • Exit codes     │  │ • Trace timing   │  │ • HTTP 8888  │ │
│  │ • Logging        │  │ • Critical path  │  │ • JSON       │ │
│  │ • Subprocess     │  │ • Dry-run        │  │ • HTML/CSS   │ │
│  │   management     │  │ • 8 commands     │  │ • JavaScript │ │
│  └──────────────────┘  └──────────────────┘  └──────────────┘ │
│         CLI                   CLI                   Web         │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Underlying Execution Layer (Java/Scala/ZIO)             │  │
│  │ • Process execution                                      │  │
│  │ • File system polling                                   │  │
│  │ • HTTP server                                           │  │
│  │ • JSON serialization                                    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
orchestrator/
├── src/main/scala/orchestrator/
│   ├── PipelineOrchestrator.scala    (193 lines) - Main orchestrator
│   ├── PipelineDAG.scala             (358 lines) - DAG model & visualization
│   └── WebUI.scala                   (400+ lines) - Web server & dashboard
│
├── src/main/resources/
│   └── log4j2.properties             (21 lines) - Logging config
│
├── README.md                         - Module overview
├── DAG.md                            - DAG visualization guide
├── WEBUI.md                          - Web UI documentation
└── TOOLS.md                          - This file
```

---

## Command Reference

### Pipeline Execution
```bash
# Run with defaults (100 events)
sbt "orchestrator/runMain orchestrator.OrchestratorMain"

# Run with custom event count
sbt "orchestrator/runMain orchestrator.OrchestratorMain 500"
```

### DAG Visualization
```bash
# 8 different visualization modes
sbt "orchestrator/runMain orchestrator.PipelineDAGMain dag"
sbt "orchestrator/runMain orchestrator.PipelineDAGMain dependencies"
sbt "orchestrator/runMain orchestrator.PipelineDAGMain timeline"
sbt "orchestrator/runMain orchestrator.PipelineDAGMain trace"
sbt "orchestrator/runMain orchestrator.PipelineDAGMain critical"
sbt "orchestrator/runMain orchestrator.PipelineDAGMain stages"
sbt "orchestrator/runMain orchestrator.PipelineDAGMain dry-run"
sbt "orchestrator/runMain orchestrator.PipelineDAGMain all"
```

### Web UI
```bash
# Start server (runs indefinitely)
sbt "orchestrator/runMain orchestrator.WebUIMain"

# In another terminal, access
curl http://localhost:8888/
open http://localhost:8888/  # macOS
```

### API Calls
```bash
# View all data
curl http://localhost:8888/api/dag
curl http://localhost:8888/api/timeline
curl http://localhost:8888/api/stages
curl http://localhost:8888/api/trace
curl http://localhost:8888/api/critical

# Health check
curl http://localhost:8888/health

# Process JSON
curl -s http://localhost:8888/api/dag | jq '.data'
```

---

## Performance Metrics

### Startup Times
- PipelineOrchestrator: ~5 seconds
- PipelineDAG: ~2 seconds
- WebUI: ~2 seconds

### Execution Times
- PublishToKafka: ~8 seconds
- SubmitFlinkJob: ~33 seconds
- WaitForParquetOutput: ~30 seconds (average)
- RunSparkETL: ~55 seconds

**Total**: ~126 seconds (~2 minutes)

### Resource Usage
- Memory: 500MB - 1GB
- CPU: <10% during execution
- Network: <1Mbps average

---

## Troubleshooting

### Tool won't start
```bash
# Check compilation
sbt "orchestrator/compile"

# Check dependencies
sbt "orchestrator/dependencyTree"
```

### Port already in use (8888)
```bash
# Find process
lsof -i :8888

# Kill it
kill -9 <PID>
```

### Kafka/Flink connection errors
```bash
# Ensure infrastructure is running
docker-compose up -d kafka flink-jobmanager flink-taskmanager

# Check logs
docker-compose logs kafka
docker-compose logs flink-jobmanager
```

### Data not appearing
```bash
# Check Kafka topic
kafka-topics --bootstrap-server localhost:9092 --list

# Check Flink job status
curl http://localhost:8081/v1/jobs

# Check data lake
ls -R data/streaming/people/
```

---

## Next Steps

1. **Understand**: `sbt "orchestrator/runMain orchestrator.PipelineDAGMain dag"`
2. **Monitor**: `sbt "orchestrator/runMain orchestrator.WebUIMain"`
3. **Execute**: `sbt "orchestrator/runMain orchestrator.OrchestratorMain"`
4. **Analyze**: `sbt "orchestrator/runMain orchestrator.PipelineDAGMain trace"`

---

**Complete toolkit for Scala 3 data pipeline orchestration with ZIO!** 🚀