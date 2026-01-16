package orchestrator

import zio.*
import com.sun.net.httpserver.{HttpServer, HttpHandler, HttpExchange}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.io.Source

/**
 * Web UI for Pipeline Orchestrator
 *
 * Serves an interactive dashboard at http://localhost:8888/
 *
 * Routes:
 *   GET  /                 - Dashboard (HTML)
 *   GET  /api/dag          - DAG JSON
 *   GET  /api/timeline     - Timeline JSON
 *   GET  /api/stages       - Stages JSON
 *   GET  /api/trace        - Execution trace JSON
 *   GET  /api/critical     - Critical path JSON
 */
object WebUI:

  private val PORT = 8888
  private val LOCALHOST = "localhost"

  /**
   * HTML Dashboard template
   */
  private def dashboardHTML: String =
    """<!DOCTYPE html>
      |<html lang="en">
      |<head>
      |    <meta charset="UTF-8">
      |    <meta name="viewport" content="width=device-width, initial-scale=1.0">
      |    <title>Pipeline Orchestrator - ZIO DAG</title>
      |    <style>
      |        * {
      |            margin: 0;
      |            padding: 0;
      |            box-sizing: border-box;
      |        }
      |
      |        body {
      |            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
      |            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      |            min-height: 100vh;
      |            padding: 20px;
      |        }
      |
      |        .container {
      |            max-width: 1400px;
      |            margin: 0 auto;
      |        }
      |
      |        header {
      |            text-align: center;
      |            color: white;
      |            margin-bottom: 40px;
      |        }
      |
      |        header h1 {
      |            font-size: 2.5em;
      |            margin-bottom: 10px;
      |        }
      |
      |        header p {
      |            font-size: 1.1em;
      |            opacity: 0.9;
      |        }
      |
      |        .dashboard {
      |            display: grid;
      |            grid-template-columns: 1fr 1fr;
      |            gap: 20px;
      |            margin-bottom: 20px;
      |        }
      |
      |        .card {
      |            background: white;
      |            border-radius: 12px;
      |            padding: 25px;
      |            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.2);
      |            transition: transform 0.3s ease, box-shadow 0.3s ease;
      |        }
      |
      |        .card:hover {
      |            transform: translateY(-5px);
      |            box-shadow: 0 15px 50px rgba(0, 0, 0, 0.3);
      |        }
      |
      |        .card h2 {
      |            color: #667eea;
      |            margin-bottom: 20px;
      |            font-size: 1.5em;
      |            border-bottom: 3px solid #667eea;
      |            padding-bottom: 10px;
      |        }
      |
      |        .dag-container {
      |            grid-column: 1 / -1;
      |            background: white;
      |            border-radius: 12px;
      |            padding: 30px;
      |            box-shadow: 0 10px 40px rgba(0, 0, 0, 0.2);
      |        }
      |
      |        .dag-container h2 {
      |            color: #667eea;
      |            margin-bottom: 20px;
      |            font-size: 1.8em;
      |        }
      |
      |        .dag-content {
      |            background: #f8f9fa;
      |            border: 2px solid #667eea;
      |            border-radius: 8px;
      |            padding: 20px;
      |            font-family: 'Courier New', monospace;
      |            overflow-x: auto;
      |            white-space: pre-wrap;
      |            word-wrap: break-word;
      |            color: #333;
      |            line-height: 1.6;
      |        }
      |
      |        .stage {
      |            background: #f0f7ff;
      |            border-left: 4px solid #667eea;
      |            padding: 15px;
      |            margin-bottom: 15px;
      |            border-radius: 6px;
      |        }
      |
      |        .stage h3 {
      |            color: #667eea;
      |            margin-bottom: 8px;
      |        }
      |
      |        .stage p {
      |            color: #555;
      |            font-size: 0.95em;
      |            line-height: 1.6;
      |        }
      |
      |        .timeline {
      |            position: relative;
      |            padding: 20px 0;
      |        }
      |
      |        .timeline-item {
      |            display: flex;
      |            margin-bottom: 30px;
      |            position: relative;
      |        }
      |
      |        .timeline-item::before {
      |            content: '';
      |            position: absolute;
      |            left: 24px;
      |            top: 50px;
      |            bottom: -30px;
      |            width: 2px;
      |            background: #667eea;
      |        }
      |
      |        .timeline-item:last-child::before {
      |            display: none;
      |        }
      |
      |        .timeline-marker {
      |            width: 50px;
      |            height: 50px;
      |            background: #667eea;
      |            border-radius: 50%;
      |            display: flex;
      |            align-items: center;
      |            justify-content: center;
      |            color: white;
      |            font-weight: bold;
      |            font-size: 0.9em;
      |            flex-shrink: 0;
            |            margin-right: 20px;
      |        }
      |
      |        .timeline-content {
      |            flex: 1;
      |            padding-top: 5px;
      |        }
      |
      |        .timeline-content h4 {
      |            color: #667eea;
      |            margin-bottom: 5px;
      |        }
      |
      |        .timeline-content p {
      |            color: #666;
      |            font-size: 0.95em;
      |            margin-bottom: 5px;
      |        }
      |
      |        .stats {
      |            display: grid;
      |            grid-template-columns: repeat(4, 1fr);
      |            gap: 15px;
      |            margin-bottom: 20px;
      |        }
      |
      |        .stat-box {
      |            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      |            color: white;
      |            padding: 20px;
      |            border-radius: 8px;
      |            text-align: center;
      |        }
      |
      |        .stat-box .value {
      |            font-size: 2em;
      |            font-weight: bold;
      |            margin-bottom: 5px;
      |        }
      |
      |        .stat-box .label {
      |            font-size: 0.9em;
      |            opacity: 0.9;
      |        }
      |
      |        .controls {
      |            display: flex;
      |            gap: 10px;
      |            margin-bottom: 20px;
      |            flex-wrap: wrap;
      |        }
      |
      |        button {
      |            background: #667eea;
      |            color: white;
      |            border: none;
      |            padding: 12px 24px;
      |            border-radius: 6px;
      |            cursor: pointer;
      |            font-size: 1em;
      |            transition: background 0.3s ease;
      |        }
      |
      |        button:hover {
      |            background: #764ba2;
      |        }
      |
      |        button.active {
      |            background: #764ba2;
      |        }
      |
      |        footer {
      |            text-align: center;
      |            color: white;
      |            margin-top: 40px;
      |            padding: 20px;
      |            opacity: 0.9;
      |        }
      |
      |        .loading {
      |            text-align: center;
      |            padding: 40px;
      |            color: #667eea;
      |        }
      |
      |        .error {
      |            background: #fee;
      |            color: #c33;
      |            padding: 15px;
      |            border-radius: 6px;
      |            margin-bottom: 15px;
      |            border-left: 4px solid #c33;
      |        }
      |
      |        @media (max-width: 1024px) {
      |            .dashboard {
      |                grid-template-columns: 1fr;
      |            }
      |
      |            .stats {
      |                grid-template-columns: repeat(2, 1fr);
      |            }
      |        }
      |    </style>
      |</head>
      |<body>
      |    <div class="container">
      |        <header>
      |            <h1>🎯 Pipeline Orchestrator</h1>
      |            <p>Real-time Kafka → Flink → Spark Pipeline with ZIO</p>
      |        </header>
      |
      |        <div class="stats" id="stats">
      |            <div class="stat-box">
      |                <div class="value">4</div>
      |                <div class="label">Stages</div>
      |            </div>
      |            <div class="stat-box">
      |                <div class="value">3</div>
      |                <div class="label">Dependencies</div>
      |            </div>
      |            <div class="stat-box">
      |                <div class="value">~2m</div>
      |                <div class="label">Est. Time</div>
      |            </div>
      |            <div class="stat-box">
      |                <div class="value">100</div>
      |                <div class="label">Events</div>
      |            </div>
      |        </div>
      |
      |        <div class="controls">
      |            <button onclick="loadDAG()">DAG</button>
      |            <button onclick="loadTimeline()">Timeline</button>
      |            <button onclick="loadStages()">Stages</button>
      |            <button onclick="loadTrace()">Trace</button>
      |            <button onclick="loadCritical()">Critical Path</button>
      |        </div>
      |
      |        <div class="dag-container">
      |            <h2>Pipeline Visualization</h2>
      |            <div id="content" class="dag-content">
      |                <div class="loading">Loading DAG...</div>
      |            </div>
      |        </div>
      |
      |        <div class="dashboard">
      |            <div class="card">
      |                <h2>📊 Execution Model</h2>
      |                <p><strong>Type:</strong> Sequential</p>
      |                <p><strong>Parallelization:</strong> None (each stage depends on previous)</p>
      |                <p><strong>Fault Tolerance:</strong> Stop on first failure</p>
      |                <p><strong>Total Time:</strong> ~126 seconds (2 minutes)</p>
      |            </div>
      |
      |            <div class="card">
      |                <h2>🔧 System Info</h2>
      |                <p><strong>Language:</strong> Scala 3.5.2</p>
      |                <p><strong>Framework:</strong> ZIO 2.0.20</p>
      |                <p><strong>Server:</strong> Java HttpServer</p>
      |                <p><strong>Port:</strong> 8888</p>
      |            </div>
      |        </div>
      |
      |        <footer>
      |            <p>🚀 Powered by ZIO + Scala 3 | Pure Functional Pipeline Orchestration</p>
      |        </footer>
      |    </div>
      |
      |    <script>
      |        async function loadDAG() {
      |            await loadContent('/api/dag', 'DAG');
      |        }
      |
      |        async function loadTimeline() {
      |            await loadContent('/api/timeline', 'Timeline');
      |        }
      |
      |        async function loadStages() {
      |            await loadContent('/api/stages', 'Stages');
      |        }
      |
      |        async function loadTrace() {
      |            await loadContent('/api/trace', 'Trace');
      |        }
      |
      |        async function loadCritical() {
      |            await loadContent('/api/critical', 'Critical Path');
      |        }
      |
      |        async function loadContent(endpoint, title) {
      |            const content = document.getElementById('content');
      |            content.innerHTML = '<div class="loading">Loading ' + title + '...</div>';
      |
      |            try {
      |                const response = await fetch(endpoint);
      |                const data = await response.json();
      |
      |                if (data.success) {
      |                    content.textContent = data.data;
      |                } else {
      |                    content.innerHTML = '<div class="error">Error: ' + data.error + '</div>';
      |                }
      |            } catch (error) {
      |                content.innerHTML = '<div class="error">Error loading data: ' + error.message + '</div>';
      |            }
      |        }
      |
      |        // Load DAG on page load
      |        window.addEventListener('load', loadDAG);
      |    </script>
      |</body>
      |</html>
      |""".stripMargin

  /**
   * HTTP Handler implementation
   */
  private class PipelineHandler extends HttpHandler:
    override def handle(exchange: HttpExchange): Unit =
      val path = exchange.getRequestURI.getPath

      val (status, response) = path match
        case "/" =>
          (200, dashboardHTML)

        case "/api/dag" =>
          (200, jsonResponse(PipelineDAG.visualizeDAG))

        case "/api/timeline" =>
          (200, jsonResponse(PipelineDAG.timeline))

        case "/api/stages" =>
          (200, jsonResponse(PipelineDAG.dependencyGraph))

        case "/api/trace" =>
          (200, jsonResponse(PipelineDAG.executionTrace))

        case "/api/critical" =>
          (200, jsonResponse(PipelineDAG.criticalPath))

        case "/health" =>
          (200, """{"status":"ok","timestamp":"${System.currentTimeMillis()}"}""")

        case _ =>
          (404, jsonError("Not found"))

      val bytes = response.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.set("Content-Type",
        if path.startsWith("/api") then "application/json" else "text/html; charset=utf-8"
      )
      exchange.sendResponseHeaders(status, bytes.length.toLong)
      exchange.getResponseBody.write(bytes)
      exchange.getResponseBody.close()

  private def jsonResponse(data: String): String =
    s"""{"success":true,"data":${escapeJson(data)}}"""

  private def jsonError(error: String): String =
    s"""{"success":false,"error":"$error"}"""

  private def escapeJson(str: String): String =
    "\"" + str
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t") + "\""

  /**
   * Start the web server
   */
  def start: ZIO[Any, Throwable, Unit] =
    ZIO.attempt {
      val server = HttpServer.create(InetSocketAddress(LOCALHOST, PORT), 0)
      server.createContext("/", new PipelineHandler())
      server.setExecutor(null)
      server.start()

      println(s"""
        |╔════════════════════════════════════════════════════════╗
        |║          🌐 Pipeline Web UI Started                   ║
        |╟════════════════════════════════════════════════════════╣
        |║  URL: http://$LOCALHOST:$PORT/                         ║
        |║                                                        ║
        |║  Available endpoints:                                 ║
        |║    GET /              - Dashboard (HTML)              ║
        |║    GET /api/dag       - DAG JSON                      ║
        |║    GET /api/timeline  - Timeline JSON                 ║
        |║    GET /api/stages    - Stages JSON                   ║
        |║    GET /api/trace     - Execution trace JSON          ║
        |║    GET /api/critical  - Critical path JSON            ║
        |║    GET /health        - Health check                  ║
        |║                                                        ║
        |║  Press Ctrl+C to stop                                 ║
        |╚════════════════════════════════════════════════════════╝
        |""".stripMargin)

      // Keep the server running
      Thread.currentThread().join()
    }

/**
 * Entry point for Web UI
 */
object WebUIMain extends ZIOAppDefault:
  def run =
    WebUI.start
      .catchAll { error =>
        ZIO.logError(s"Failed to start web server: ${error.getMessage}") *>
        ZIO.fail(error)
      }