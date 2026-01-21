#!/bin/bash

echo "================================================"
echo "  Starting Orchestrator Dashboard"
echo "================================================"
echo ""
echo "📊 Compiling and starting dashboard..."
echo ""

# Compile and run
sbt "orchestrator/runMain orchestrator.OrchestratorDashboard" &
DASHBOARD_PID=$!

# Wait for startup
echo "⏳ Starting up... (waiting 5 seconds)"
sleep 5

echo ""
echo "================================================"
echo "  ✅ Dashboard Started!"
echo "================================================"
echo ""
echo "🌐 Open your browser to:"
echo "   http://localhost:9090"
echo ""
echo "📋 Available Endpoints:"
echo "   GET  /                        - Dashboard UI"
echo "   GET  /api/tasks               - List all tasks"
echo "   GET  /api/metrics/<task-name> - Task metrics"
echo "   GET  /api/summary             - Execution summary"
echo "   POST /api/execute/<task-name> - Execute task"
echo ""
echo "Sample Tasks:"
echo "   • PublishToKafkaTask"
echo "   • SubmitFlinkJobTask"
echo "   • WaitForParquetOutputTask"
echo "   • RunSparkETLTask"
echo ""
echo "Example curl commands:"
echo "   curl http://localhost:9090/api/tasks"
echo "   curl -X POST http://localhost:9090/api/execute/RunSparkETLTask"
echo ""
echo "Press Ctrl+C to stop the dashboard"
echo ""

# Keep the script running
wait $DASHBOARD_PID