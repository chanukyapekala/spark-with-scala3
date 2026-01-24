package orchestrator

import zio.*
import com.sun.net.httpserver.{HttpServer, HttpHandler, HttpExchange}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

/**
 * Interactive Web-Based Orchestrator Dashboard
 *
 * Starts a web server at http://localhost:9090 with:
 * - Interactive task cards
 * - Real-time execution tracking
 * - Execution logs and metrics
 * - Task execution history
 */
object InteractiveOrchestrator:

  private val PORT = 9090
  private val LOCALHOST = "localhost"

  private def dashboardHTML: String =
    """<!DOCTYPE html>
      |<html lang="en">
      |<head>
      |    <meta charset="UTF-8">
      |    <meta name="viewport" content="width=device-width, initial-scale=1.0">
      |    <title>Pipeline Orchestrator Dashboard</title>
      |    <style>
      |        * { margin: 0; padding: 0; box-sizing: border-box; }
      |        body {
      |            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
      |            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      |            min-height: 100vh;
      |            padding: 20px;
      |        }
      |        .container { max-width: 1400px; margin: 0 auto; }
      |        .header { color: white; margin-bottom: 30px; }
      |        .header h1 { font-size: 2.5em; margin-bottom: 10px; }
      |        .header p { opacity: 0.9; font-size: 1.1em; }
      |        .dashboard { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 30px; }
      |        .summary-card {
      |            background: white;
      |            border-radius: 12px;
      |            padding: 20px;
      |            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
      |            display: flex;
      |            flex-direction: column;
      |            align-items: center;
      |        }
      |        .summary-number { font-size: 2.5em; font-weight: bold; color: #667eea; margin: 10px 0; }
      |        .summary-label { color: #666; font-size: 0.95em; text-transform: uppercase; }
      |        .tasks-grid {
      |            display: grid;
      |            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      |            gap: 20px;
      |            margin-bottom: 30px;
      |        }
      |        .task-card {
      |            background: white;
      |            border-radius: 12px;
      |            padding: 20px;
      |            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
      |            cursor: pointer;
      |            transition: all 0.3s ease;
      |            border-left: 5px solid #667eea;
      |        }
      |        .task-card:hover { transform: translateY(-5px); box-shadow: 0 15px 40px rgba(0,0,0,0.2); }
      |        .task-card.running { border-left-color: #2196F3; background: #e3f2fd; }
      |        .task-card.completed { border-left-color: #4CAF50; background: #e8f5e9; }
      |        .task-card.failed { border-left-color: #f44336; background: #ffebee; }
      |        .task-id { font-size: 1.3em; font-weight: bold; margin-bottom: 8px; }
      |        .task-type { display: inline-block; padding: 4px 12px; border-radius: 20px; font-size: 0.85em; font-weight: 600; margin: 5px 0; }
      |        .type-producer { background: #c8e6c9; color: #2e7d32; }
      |        .type-processor { background: #bbdefb; color: #1565c0; }
      |        .type-sensor { background: #ffe0b2; color: #e65100; }
      |        .type-analyzer { background: #e1bee7; color: #6a1b9a; }
      |        .task-status { font-size: 0.9em; margin-top: 10px; color: #666; }
      |        .status-badge {
      |            display: inline-block;
      |            padding: 4px 10px;
      |            border-radius: 4px;
      |            font-weight: 600;
      |            font-size: 0.85em;
      |        }
      |        .status-pending { background: #f0f0f0; color: #333; }
      |        .status-running { background: #2196F3; color: white; }
      |        .status-completed { background: #4CAF50; color: white; }
      |        .status-failed { background: #f44336; color: white; }
      |        .task-duration { font-size: 0.9em; color: #999; margin-top: 8px; }
      |        .modal {
      |            display: none;
      |            position: fixed;
      |            top: 0;
      |            left: 0;
      |            right: 0;
      |            bottom: 0;
      |            background: rgba(0,0,0,0.7);
      |            z-index: 1000;
      |            align-items: center;
      |            justify-content: center;
      |        }
      |        .modal.active { display: flex; }
      |        .modal-content {
      |            background: white;
      |            border-radius: 12px;
      |            width: 90%;
      |            max-width: 800px;
      |            max-height: 80vh;
      |            overflow-y: auto;
      |            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
      |        }
      |        .modal-header {
      |            padding: 20px;
      |            border-bottom: 1px solid #eee;
      |            display: flex;
      |            justify-content: space-between;
      |            align-items: center;
      |        }
      |        .modal-title { font-size: 1.5em; font-weight: bold; }
      |        .close-btn {
      |            background: none;
      |            border: none;
      |            font-size: 2em;
      |            cursor: pointer;
      |            color: #666;
      |        }
      |        .modal-body { padding: 20px; }
      |        .detail-section { margin-bottom: 20px; }
      |        .detail-label { font-weight: bold; color: #667eea; margin-bottom: 5px; font-size: 0.95em; }
      |        .logs-container {
      |            background: #f5f5f5;
      |            border-radius: 8px;
      |            padding: 15px;
      |            font-family: 'Courier New', monospace;
      |            font-size: 0.9em;
      |            max-height: 300px;
      |            overflow-y: auto;
      |            line-height: 1.5;
      |        }
      |        .log-line {
      |            padding: 5px 0;
      |            border-bottom: 1px solid #e0e0e0;
      |        }
      |        .log-time { color: #999; margin-right: 10px; }
      |        .log-info { color: #333; }
      |        .log-warn { color: #ff9800; }
      |        .log-error { color: #f44336; }
      |        .action-buttons {
      |            display: flex;
      |            gap: 10px;
      |            margin-top: 20px;
      |        }
      |        .btn {
      |            flex: 1;
      |            padding: 12px 20px;
      |            border: none;
      |            border-radius: 6px;
      |            font-weight: 600;
      |            cursor: pointer;
      |            transition: all 0.3s ease;
      |        }
      |        .btn-primary { background: #667eea; color: white; }
      |        .btn-primary:hover { background: #5568d3; transform: translateY(-2px); }
      |        .btn-secondary { background: #f0f0f0; color: #333; }
      |        .btn-secondary:hover { background: #e0e0e0; }
      |        .metrics {
      |            display: grid;
      |            grid-template-columns: repeat(3, 1fr);
      |            gap: 10px;
      |            margin: 15px 0;
      |        }
      |        .metric-box {
      |            background: #f9f9f9;
      |            padding: 12px;
      |            border-radius: 6px;
      |            text-align: center;
      |        }
      |        .metric-value { font-size: 1.5em; font-weight: bold; color: #667eea; }
      |        .metric-label { font-size: 0.85em; color: #999; margin-top: 5px; }
      |        #dagContainer { position: relative; }
      |        svg.dag-arrows { position: absolute; top: 0; left: 0; width: 100%; height: 100%; pointer-events: none; z-index: 1; }
      |        .task-card { position: relative; z-index: 2; }
      |        .arrow-line { stroke: #667eea; stroke-width: 2; fill: none; }
      |        .arrow-head { fill: #667eea; }
      |    </style>
      |</head>
      |<body>
      |    <div class="container">
      |        <div class="header">
      |            <h1>🎯 Pipeline Orchestrator</h1>
      |            <p style="font-size: 1.3em; margin-bottom: 15px; font-weight: 500;">📊 Scala Workflow: Real-time Streaming + Batch Analytics</p>
      |            <p>Interactive Task DAG - Click on tasks to view details</p>
      |        </div>
      |
      |        <div class="dashboard" id="summary">
      |            <div class="summary-card">
      |                <div class="summary-label">Tasks</div>
      |                <div class="summary-number" id="total-tasks">-</div>
      |            </div>
      |            <div class="summary-card">
      |                <div class="summary-label">Completed</div>
      |                <div class="summary-number" id="completed-tasks" style="color: #4CAF50;">-</div>
      |            </div>
      |            <div class="summary-card">
      |                <div class="summary-label">Running</div>
      |                <div class="summary-number" id="running-tasks" style="color: #2196F3;">-</div>
      |            </div>
      |            <div class="summary-card">
      |                <div class="summary-label">Failed</div>
      |                <div class="summary-number" id="failed-tasks" style="color: #f44336;">-</div>
      |            </div>
      |        </div>
      |
      |        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
      |            <h2 style="color: white; margin: 0;">📦 Tasks</h2>
      |            <button onclick="runEntireWorkflow()" style="padding: 10px 20px; background: #4CAF50; color: white; border: none; border-radius: 6px; cursor: pointer; font-weight: bold; font-size: 1em;">▶ Run Workflow</button>
      |        </div>
      |        <div id="dagContainer" style="position: relative;">
      |            <svg class="dag-arrows" id="dagArrows"></svg>
      |            <div class="tasks-grid" id="tasks-container"></div>
      |        </div>
      |    </div>
      |
      |    <div class="modal" id="workflowModal">
      |        <div class="modal-content">
      |            <div class="modal-header">
      |                <div class="modal-title">🔄 Lambda Architecture Execution: Kafka → Flink → Spark</div>
      |                <button class="close-btn" onclick="closeWorkflowModal()">✕</button>
      |            </div>
      |            <div class="modal-body">
      |                <div id="workflowStatus"></div>
      |            </div>
      |        </div>
      |    </div>
      |
      |    <div class="modal" id="taskModal">
      |        <div class="modal-content">
      |            <div class="modal-header">
      |                <div class="modal-title" id="modalTaskId"></div>
      |                <button class="close-btn" onclick="closeModal()">✕</button>
      |            </div>
      |            <div class="modal-body">
      |                <div class="detail-section">
      |                    <div class="detail-label">Status</div>
      |                    <div id="modalStatus"></div>
      |                </div>
      |                <div class="detail-section">
      |                    <div class="detail-label">Metrics</div>
      |                    <div class="metrics">
      |                        <div class="metric-box">
      |                            <div class="metric-value" id="modalDuration">-</div>
      |                            <div class="metric-label">Duration (ms)</div>
      |                        </div>
      |                        <div class="metric-box">
      |                            <div class="metric-value" id="modalRecords">-</div>
      |                            <div class="metric-label">Records</div>
      |                        </div>
      |                        <div class="metric-box">
      |                            <div class="metric-value" id="modalErrors">-</div>
      |                            <div class="metric-label">Errors</div>
      |                        </div>
      |                    </div>
      |                </div>
      |                <div class="detail-section">
      |                    <div class="detail-label">📋 Sample Data (spark.show)</div>
      |                    <div class="sample-data-container" id="modalSampleData" style="background: #f9f9f9; padding: 12px; border-radius: 8px; font-size: 0.8em; overflow-x: auto;"><table style="width: 100%; border-collapse: collapse; font-family: monospace;"><tbody></tbody></table></div>
      |                </div>
      |                <div class="detail-section">
      |                    <div class="detail-label">📊 Task Metadata</div>
      |                    <div class="task-data-container" id="modalTaskData" style="display: grid; grid-template-columns: 1fr 1fr; gap: 12px; background: #f9f9f9; padding: 15px; border-radius: 8px; font-size: 0.9em;"></div>
      |                </div>
      |                <div class="detail-section">
      |                    <div class="detail-label">Logs</div>
      |                    <div class="logs-container" id="modalLogs"></div>
      |                </div>
      |                <div class="action-buttons">
      |                    <button class="btn btn-primary" onclick="runTask()" id="runBtn">▶ Run Task</button>
      |                    <button class="btn btn-secondary" onclick="closeModal()">Close</button>
      |                </div>
      |            </div>
      |        </div>
      |    </div>
      |
      |    <script>
      |        let currentTask = null;
      |        let autoRefresh = null;
      |
      |        async function fetchTasks() {
      |            try {
      |                const response = await fetch('/api/tasks');
      |                return await response.json();
      |            } catch (e) {
      |                console.error('Error fetching tasks:', e);
      |                return [];
      |            }
      |        }
      |
      |        async function fetchMetrics(taskId) {
      |            try {
      |                const response = await fetch(`/api/metrics/${taskId}`);
      |                return await response.json();
      |            } catch (e) {
      |                console.error('Error fetching metrics:', e);
      |                return null;
      |            }
      |        }
      |
      |        async function fetchSummary() {
      |            try {
      |                const response = await fetch('/api/summary');
      |                return await response.json();
      |            } catch (e) {
      |                console.error('Error fetching summary:', e);
      |                return null;
      |            }
      |        }
      |
      |        async function renderTasks() {
      |            const tasks = await fetchTasks();
      |            const summary = await fetchSummary();
      |
      |            if (summary) {
      |                document.getElementById('total-tasks').textContent = summary.total || 0;
      |                document.getElementById('completed-tasks').textContent = summary.completed || 0;
      |                document.getElementById('running-tasks').textContent = summary.running || 0;
      |                document.getElementById('failed-tasks').textContent = summary.failed || 0;
      |            }
      |
      |            const container = document.getElementById('tasks-container');
      |            container.innerHTML = '';
      |
      |            for (const task of tasks) {
      |                const metrics = await fetchMetrics(task.id);
      |                const statusClass = metrics?.status?.toLowerCase() || 'pending';
      |                const statusBadge = metrics?.status || 'PENDING';
      |                const typeClass = `type-${task.taskType.toLowerCase()}`;
      |
      |                const card = document.createElement('div');
      |                card.className = `task-card ${statusClass}`;
      |                card.onclick = () => openModal(task.id);
      |
      |                const duration = metrics?.duration ? `${metrics.duration}ms` : 'Not started';
      |
      |                card.innerHTML = `
      |                    <div class="task-id">${task.id}</div>
      |                    <div class="task-type ${typeClass}">${task.taskType}</div>
      |                    <div style="font-size: 0.9em; color: #666; margin-top: 10px;">${task.description}</div>
      |                    <div class="task-status">
      |                        <span class="status-badge status-${statusClass}">${statusBadge}</span>
      |                    </div>
      |                    <div class="task-duration">⏱ ${duration}</div>
      |                    ${metrics?.recordsProcessed ? `<div style="font-size: 0.9em; color: #666; margin-top: 5px;">📊 ${metrics.recordsProcessed} records</div>` : ''}
      |                `;
      |                container.appendChild(card);
      |            }
      |
      |            // Draw arrows between dependent tasks
      |            drawTaskArrows(tasks);
      |        }
      |
      |        function drawTaskArrows(tasks) {
      |            setTimeout(() => {
      |                const svg = document.getElementById('dagArrows');
      |                const container = document.getElementById('dagContainer');
      |                svg.innerHTML = '';
      |
      |                const cards = container.querySelectorAll('.task-card');
      |                const cardMap = {};
      |
      |                // Map task IDs to their card elements
      |                cards.forEach(card => {
      |                    const taskId = card.querySelector('.task-id').textContent;
      |                    cardMap[taskId] = card;
      |                });
      |
      |                // Draw arrows for each dependency
      |                tasks.forEach(task => {
      |                    const fromCard = cardMap[task.id];
      |                    if (!fromCard) return;
      |
      |                    task.dependencies.forEach(depId => {
      |                        const toCard = cardMap[depId];
      |                        if (!toCard) return;
      |
      |                        const fromRect = fromCard.getBoundingClientRect();
      |                        const toRect = toCard.getBoundingClientRect();
      |                        const containerRect = container.getBoundingClientRect();
      |
      |                        // Calculate positions relative to container
      |                        const fromX = fromRect.left - containerRect.left + fromRect.width / 2;
      |                        const fromY = fromRect.top - containerRect.top;
      |                        const toX = toRect.left - containerRect.left + toRect.width / 2;
      |                        const toY = toRect.top - containerRect.top + toRect.height;
      |
      |                        // Create curved path
      |                        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
      |                        const controlY = (fromY + toY) / 2;
      |                        const pathData = `M ${fromX} ${fromY} C ${fromX} ${controlY}, ${toX} ${controlY}, ${toX} ${toY}`;
      |
      |                        path.setAttribute('d', pathData);
      |                        path.setAttribute('class', 'arrow-line');
      |                        path.setAttribute('stroke', '#667eea');
      |                        path.setAttribute('stroke-width', '3');
      |                        path.setAttribute('fill', 'none');
      |                        path.setAttribute('stroke-linecap', 'round');
      |
      |                        svg.appendChild(path);
      |
      |                        // Add arrowhead
      |                        const angle = Math.atan2(toY - controlY, toX - fromX);
      |                        const arrowSize = 12;
      |                        const arrowPoints = [
      |                            [toX - arrowSize * Math.cos(angle - Math.PI / 6), toY - arrowSize * Math.sin(angle - Math.PI / 6)],
      |                            [toX, toY],
      |                            [toX - arrowSize * Math.cos(angle + Math.PI / 6), toY - arrowSize * Math.sin(angle + Math.PI / 6)]
      |                        ];
      |
      |                        const polygon = document.createElementNS('http://www.w3.org/2000/svg', 'polygon');
      |                        polygon.setAttribute('points', arrowPoints.map(p => p.join(',')).join(' '));
      |                        polygon.setAttribute('fill', '#667eea');
      |                        svg.appendChild(polygon);
      |                    });
      |                });
      |
      |                // Set SVG dimensions
      |                svg.setAttribute('height', container.scrollHeight);
      |            }, 100);
      |        }
      |
      |        async function runEntireWorkflow() {
      |            if (confirm('Run all tasks in dependency order?')) {
      |                const tasks = await fetchTasks();
      |                let taskIndex = 0;
      |
      |                const executeNextTask = async () => {
      |                    if (taskIndex >= tasks.length) {
      |                        document.getElementById('workflowStatus').innerHTML = '<div style="padding: 20px; text-align: center; color: #4CAF50;"><b>✅ Workflow complete!</b></div>';
      |                        return;
      |                    }
      |                    const task = tasks[taskIndex];
      |                    document.getElementById('workflowStatus').innerHTML = `<div style="padding: 20px; text-align: center;"><b>Running ${taskIndex + 1}/${tasks.length}: ${task.id}</b></div>`;
      |                    try {
      |                        await fetch(`/api/execute/${task.id}`, { method: 'POST' });
      |                        await new Promise(r => setTimeout(r, 2000));
      |                        taskIndex++;
      |                        executeNextTask();
      |                    } catch (e) {
      |                        console.error('Error running task:', e);
      |                    }
      |                };
      |                document.getElementById('workflowModal').classList.add('active');
      |                executeNextTask();
      |            }
      |        }
      |
      |        async function openModal(taskId) {
      |            currentTask = taskId;
      |            const metrics = await fetchMetrics(taskId);
      |
      |            document.getElementById('modalTaskId').textContent = taskId;
      |            document.getElementById('modalStatus').innerHTML = `<span class="status-badge status-${metrics?.status?.toLowerCase() || 'pending'}">${metrics?.status || 'PENDING'}</span>`;
      |            document.getElementById('modalDuration').textContent = metrics?.duration || '-';
      |            document.getElementById('modalRecords').textContent = metrics?.recordsProcessed || '-';
      |            document.getElementById('modalErrors').textContent = metrics?.recordsFailed || '0';
      |
      |            // Display sample data as table
      |            if (metrics?.sampleData && metrics.sampleData.length > 0) {
      |                let columns = [];
      |                metrics.sampleData.forEach(row => {
      |                    Object.keys(row).forEach(col => {
      |                        if (!columns.includes(col)) columns.push(col);
      |                    });
      |                });
      |
      |                let tableHtml = '<table style="width: 100%; border-collapse: collapse; font-family: monospace;"><thead><tr style="background: #f0f0f0; border-bottom: 2px solid #ddd;">';
      |                columns.forEach(col => {
      |                    tableHtml += `<th style="padding: 8px; text-align: left; font-weight: bold; color: #333; border-right: 1px solid #ddd;">${col}</th>`;
      |                });
      |                tableHtml += '</tr></thead><tbody>';
      |
      |                metrics.sampleData.forEach((row, idx) => {
      |                    tableHtml += `<tr style="border-bottom: 1px solid #eee; ${idx % 2 === 0 ? 'background: #fafafa;' : ''}">`;
      |                    columns.forEach(col => {
      |                        tableHtml += `<td style="padding: 8px; border-right: 1px solid #eee; color: #555;">${row[col] || '-'}</td>`;
      |                    });
      |                    tableHtml += '</tr>';
      |                });
      |                tableHtml += '</tbody></table>';
      |                document.querySelector('#modalSampleData table tbody').parentElement.innerHTML = tableHtml;
      |            } else {
      |                document.querySelector('#modalSampleData table tbody').parentElement.innerHTML = '<div style="color: #999; text-align: center; padding: 20px;">No sample data available</div>';
      |            }
      |
      |            // Display task data
      |            const taskDataHtml = Object.entries(metrics?.taskData || {}).map(([key, value]) => {
      |                return `<div style="border-bottom: 1px solid #e0e0e0; padding-bottom: 8px;"><strong>${key}:</strong> <span style="color: #555;">${value}</span></div>`;
      |            }).join('');
      |            document.getElementById('modalTaskData').innerHTML = taskDataHtml || '<div style="color: #999; text-align: center; padding: 20px;">No task metadata available</div>';
      |
      |            const logsHtml = (metrics?.logs || []).map(log => {
      |                const levelClass = `log-${log.level.toLowerCase()}`;
      |                return `<div class="log-line"><span class="log-time">${log.timestamp.slice(11, 19)}</span><span class="${levelClass}">${log.message}</span></div>`;
      |            }).join('');
      |
      |            document.getElementById('modalLogs').innerHTML = logsHtml || '<div style="color: #999; padding: 20px; text-align: center;">No logs yet</div>';
      |
      |            document.getElementById('taskModal').classList.add('active');
      |
      |            if (autoRefresh) clearInterval(autoRefresh);
      |            autoRefresh = setInterval(() => {
      |                if (document.getElementById('taskModal').classList.contains('active')) {
      |                    refreshModalData(taskId);
      |                }
      |            }, 500);
      |        }
      |
      |        async function refreshModalData(taskId) {
      |            const metrics = await fetchMetrics(taskId);
      |            if (metrics) {
      |                document.getElementById('modalStatus').innerHTML = `<span class="status-badge status-${metrics.status.toLowerCase()}">${metrics.status}</span>`;
      |                document.getElementById('modalDuration').textContent = metrics.duration || '-';
      |                document.getElementById('modalRecords').textContent = metrics.recordsProcessed || '-';
      |                document.getElementById('modalErrors').textContent = metrics.recordsFailed || '0';
      |
      |                // Update sample data as table
      |                if (metrics?.sampleData && metrics.sampleData.length > 0) {
      |                    let columns = [];
      |                    metrics.sampleData.forEach(row => {
      |                        Object.keys(row).forEach(col => {
      |                            if (!columns.includes(col)) columns.push(col);
      |                        });
      |                    });
      |
      |                    let tableHtml = '<table style="width: 100%; border-collapse: collapse; font-family: monospace;"><thead><tr style="background: #f0f0f0; border-bottom: 2px solid #ddd;">';
      |                    columns.forEach(col => {
      |                        tableHtml += `<th style="padding: 8px; text-align: left; font-weight: bold; color: #333; border-right: 1px solid #ddd;">${col}</th>`;
      |                    });
      |                    tableHtml += '</tr></thead><tbody>';
      |
      |                    metrics.sampleData.forEach((row, idx) => {
      |                        tableHtml += `<tr style="border-bottom: 1px solid #eee; ${idx % 2 === 0 ? 'background: #fafafa;' : ''}">`;
      |                        columns.forEach(col => {
      |                            tableHtml += `<td style="padding: 8px; border-right: 1px solid #eee; color: #555;">${row[col] || '-'}</td>`;
      |                        });
      |                        tableHtml += '</tr>';
      |                    });
      |                    tableHtml += '</tbody></table>';
      |                    document.querySelector('#modalSampleData table tbody').parentElement.innerHTML = tableHtml;
      |                } else {
      |                    document.querySelector('#modalSampleData table tbody').parentElement.innerHTML = '<div style="color: #999; text-align: center; padding: 20px;">No sample data available</div>';
      |                }
      |
      |                // Update task data
      |                const taskDataHtml = Object.entries(metrics?.taskData || {}).map(([key, value]) => {
      |                    return `<div style="border-bottom: 1px solid #e0e0e0; padding-bottom: 8px;"><strong>${key}:</strong> <span style="color: #555;">${value}</span></div>`;
      |                }).join('');
      |                document.getElementById('modalTaskData').innerHTML = taskDataHtml || '<div style="color: #999; text-align: center; padding: 20px;">No task metadata available</div>';
      |
      |                const logsHtml = (metrics.logs || []).map(log => {
      |                    const levelClass = `log-${log.level.toLowerCase()}`;
      |                    return `<div class="log-line"><span class="log-time">${log.timestamp.slice(11, 19)}</span><span class="${levelClass}">${log.message}</span></div>`;
      |                }).join('');
      |
      |                document.getElementById('modalLogs').innerHTML = logsHtml;
      |            }
      |        }
      |
      |        function closeModal() {
      |            document.getElementById('taskModal').classList.remove('active');
      |            if (autoRefresh) clearInterval(autoRefresh);
      |        }
      |
      |        function closeWorkflowModal() {
      |            document.getElementById('workflowModal').classList.remove('active');
      |        }
      |
      |        async function runTask() {
      |            if (!currentTask) return;
      |            document.getElementById('runBtn').disabled = true;
      |            document.getElementById('runBtn').textContent = 'Running...';
      |
      |            try {
      |                const response = await fetch(`/api/execute/${currentTask}`, { method: 'POST' });
      |                if (response.ok) {
      |                    await refreshModalData(currentTask);
      |                }
      |            } catch (e) {
      |                console.error('Error running task:', e);
      |            } finally {
      |                document.getElementById('runBtn').disabled = false;
      |                document.getElementById('runBtn').textContent = '▶ Run Task';
      |            }
      |        }
      |
      |        // Initial render and refresh
      |        renderTasks();
      |        setInterval(renderTasks, 2000);
      |    </script>
      |</body>
      |</html>""".stripMargin

  def start: ZIO[Any, Throwable, Unit] =
    ZIO.attempt {
      val server = HttpServer.create(InetSocketAddress(LOCALHOST, PORT), 0)

      // Dashboard route
      server.createContext("/", new HttpHandler {
        override def handle(exchange: HttpExchange): Unit =
          val response = dashboardHTML.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.set("Content-Type", "text/html; charset=UTF-8")
          exchange.sendResponseHeaders(200, response.length)
          exchange.getResponseBody.write(response)
          exchange.close()
      })

      // API: Get all tasks
      server.createContext("/api/tasks", new HttpHandler {
        override def handle(exchange: HttpExchange): Unit =
          val json = AllTasks.tasks.map { task =>
            s"""{"id":"${task.id}","description":"${task.description}","taskType":"${task.taskType}","estimatedDuration":${task.estimatedDuration},"dependencies":[${task.dependsOn.map(d => s""""$d"""").mkString(",")}]}"""
          }.mkString("[", ",", "]")
          val response = json.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.set("Content-Type", "application/json")
          exchange.sendResponseHeaders(200, response.length)
          exchange.getResponseBody.write(response)
          exchange.close()
      })

      // API: Get task metrics
      server.createContext("/api/metrics/", new HttpHandler {
        override def handle(exchange: HttpExchange): Unit =
          val taskId = exchange.getRequestURI.toString.replace("/api/metrics/", "")
          val json = TaskExecutionTracker.getMetrics(taskId) match
            case Some(metrics) => metrics.toJson
            case None => s"""{"taskId":"$taskId","status":"PENDING","duration":0,"recordsProcessed":0,"recordsFailed":0,"logs":[]}"""
          val response = json.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.set("Content-Type", "application/json")
          exchange.sendResponseHeaders(200, response.length)
          exchange.getResponseBody.write(response)
          exchange.close()
      })

      // API: Get summary
      server.createContext("/api/summary", new HttpHandler {
        override def handle(exchange: HttpExchange): Unit =
          val json = TaskExecutionTracker.getExecutionSummary
          val response = json.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.set("Content-Type", "application/json")
          exchange.sendResponseHeaders(200, response.length)
          exchange.getResponseBody.write(response)
          exchange.close()
      })

      // API: Execute task (run in background thread)
      server.createContext("/api/execute/", new HttpHandler {
        override def handle(exchange: HttpExchange): Unit =
          if exchange.getRequestMethod == "POST" then
            val taskId = exchange.getRequestURI.toString.replace("/api/execute/", "")
            try
              // Send immediate response
              val response = s"""{"status":"queued","taskId":"$taskId"}""".getBytes(StandardCharsets.UTF_8)
              exchange.getResponseHeaders.set("Content-Type", "application/json")
              exchange.sendResponseHeaders(202, response.length)
              exchange.getResponseBody.write(response)
              exchange.close()

              // Run task asynchronously in background thread
              new Thread {
                setDaemon(true)
                override def run(): Unit =
                  try
                    TaskExecutionTracker.initializeTask(taskId)
                    TaskExecutionTracker.startTask(taskId)

                    // Get and execute the actual task
                    AllTasks.getTask(taskId) match
                      case Some(task) =>
                        try
                          // Run the actual task (this populates taskData)
                          import zio._
                          import zio.Runtime.default
                          val runtime = Runtime.default
                          val result = runtime.unsafe.run(task.run).getOrThrowFiberFailure()

                          // Update tracker with results
                          TaskExecutionTracker.updateProgress(taskId, result.duration / 1000) // Convert to seconds
                          if result.success then
                            TaskExecutionTracker.completeTask(taskId, true)
                          else
                            TaskExecutionTracker.completeTask(taskId, false, result.error)
                        catch
                          case e: Exception =>
                            TaskExecutionTracker.addLog(taskId, s"Task execution error: ${e.getMessage}", "ERROR")
                            TaskExecutionTracker.completeTask(taskId, false, Some(e.getMessage))
                      case None =>
                        TaskExecutionTracker.completeTask(taskId, false, Some(s"Task not found: $taskId"))
                  catch
                    case e: Exception =>
                      TaskExecutionTracker.completeTask(taskId, false, Some(e.getMessage))
              }.start()
            catch
              case e: Exception =>
                try
                  val error = s"""{"error":"${e.getMessage}"}"""
                  val resp = error.getBytes(StandardCharsets.UTF_8)
                  exchange.getResponseHeaders.set("Content-Type", "application/json")
                  exchange.sendResponseHeaders(500, resp.length)
                  exchange.getResponseBody.write(resp)
                catch
                  case _ => ()
                finally
                  exchange.close()
          else
            exchange.sendResponseHeaders(405, 0)
            exchange.close()
      })

      server.setExecutor(null)
      server.start()
      println(s"✅ Interactive Dashboard started at http://$LOCALHOST:$PORT")
    }