import express from "express";
import { WebSocketServer } from "ws";
import { v4 as uuidv4 } from "uuid";

const app = express();
const port = 3000;

app.use(express.json());

// Map to store WebSocket connections by task ID
const taskClients = new Map();

// Map to store WebSocket connections by worker ID
const workerServers = new Map();

// Initialize WebSocket server
const wss = new WebSocketServer({ noServer: true });

// Handle new WebSocket connections
wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const taskId = url.searchParams.get("task_id");
  const workerId = url.searchParams.get("worker_id");

  if (taskId) {
    console.log(`🔌 Client connected for task ${taskId}`);
    const prevValue = taskClients.get(taskId);
    prevValue.ws = ws; // Update existing client connection
    taskClients.set(taskId, prevValue);

    ws.on("close", () => {
      console.log(`❌ Client disconnected from task ${taskId}`);
      taskClients.delete(taskId);
    });
  } else if (workerId) {
    console.log(`🔌 Worker connected with ID ${workerId}`);
    const prevWorker = workerServers.get(workerId);
    prevWorker.ws = ws; // Update existing worker connection
    workerServers.set(workerId, prevWorker);

    ws.on("close", () => {
      console.log(`❌ Worker disconnected with ID ${workerId}`);
      workerServers.delete(workerId);
    });
  } else {
    console.error("❌ WebSocket connection without task_id or worker_id");
    ws.close();
  }
});

// Handle task submissions from clients
app.post("/register_task", async (req, res) => {
  const { code, args } = req.body;

  if (!code || !Array.isArray(args) || args.length === 0) {
    return res.status(400).json({ error: "Missing code or arguments." });
  }

  const taskId = uuidv4(); // Generate unique task ID

  try {
    res.json({ message: "Task registered successfully", task_id: taskId });
  } catch (err) {
    console.error("❌ Error registering task:", err.message);
    return res.status(500).json({ error: "Failed to register task" });
  }
  const newClient = {
    numTasks: args.length,
  };
  taskClients.set(taskId, newClient); // Initialize task client map entry
  console.log(`🔌 Task registered with ID: ${taskId}`);
  // Round-robin tasks assignment
  for (const arg of args) {
    workerServers.forEach((worker) => {
      if (worker.availableWorkers > 0) {
        const infoTask = {
          code,
          arg,
          taskId,
        };
        worker.ws.send(JSON.stringify(infoTask), (err) => {
          if (err) {
            console.error(
              `❌ Error sending task to worker ${worker.id}:`,
              err.message
            );
          } else {
            console.log(`✅ Task sent to worker ${worker.id}:`, infoTask);
            worker.availableWorkers--;
          }
        });
      }
    });
  }
});

app.post("/register_worker", (req, res) => {
  const worker_id = uuidv4(); // Generate unique worker ID

  const { numWorkers } = req.body;

  const newWorker = {
    maxWorkers: numWorkers || 1,
    availableWorkers: numWorkers || 1, // Default to 1 if not provided
  };

  try {
    res.json({ message: "Worker registered successfully", worker_id });
  } catch (err) {
    console.error("❌ Error registering worker:", err.message);
    return res.status(500).json({ error: "Failed to register worker" });
  }

  workerServers.set(worker_id, newWorker);
  console.log(`🔌 Worker registered with ID: ${worker_id}`);
});

// Start HTTP server
const server = app.listen(port, () => {
  console.log(`🚀 Orchestrator listening at http://localhost:${port}`);
});

// Upgrade HTTP connections to WebSocket when needed
server.on("upgrade", (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
});
