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
    console.log(`🔌 Client WS connected for task ${taskId}`);
    const info = taskClients.get(taskId);
    info.ws = ws; // Update existing client connection
    taskClients.set(taskId, info);

    ws.on("close", () => {
      console.log(`❌ Client WS disconnected from task ${taskId}`);
      taskClients.delete(taskId);
    });
  } else if (workerId) {
    console.log(`🔌 Worker WS connected with ID ${workerId}`);
    const info = workerServers.get(workerId);
    info.ws = ws; // Update existing worker connection
    workerServers.set(workerId, info);

    ws.on("close", () => {
      console.log(`❌ Worker WS disconnected with ID ${workerId}`);
      workerServers.delete(workerId);
    });

    ws.on("message", (data) => {
      // Expect JSON: { taskId, status, result }
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch (e) {
        console.error("❌ Invalid JSON from worker:", data);
        return;
      }

      console.log(`📨 From worker ${workerId}:`, msg);
      taskExecuted(workerId, msg);
      info.availableWorkers++;
    });
  } else {
    console.error("❌ WebSocket WS connection without task_id or worker_id");
    ws.close();
  }
});

function taskExecuted(workerId, info) {
  // Send task result to the corresponding client
  const { arg, taskId, status, result } = info;
  const clientInfo = taskClients.get(taskId);
  if (!clientInfo) {
    console.error(`❌ Client disconnected for task ID ${taskId}`);
    return;
  }
  if (status === "done") {
    console.log(
      `✅ Worker ${workerId} executed successfully task ${arg}:${taskId} :`,
      result
    );
    clientInfo.ws.send(JSON.stringify({ arg, taskId, status, result }));
  } else if (status === "error") {
    console.error(
      `❌ Worker ${workerId} fail during executing task ${arg}:${taskId} :`,
      result
    );
    clientInfo.ws.send(JSON.stringify({ arg, taskId, status, result }));
  } else {
    console.error(`❌ Unknown status for task ${taskId}:`, status);
  }
}

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
              `❌ Error sending task to worker ${worker.worker_id}:`,
              err.message
            );
          } else {
            console.log(
              `✅ Task sent to worker ${worker.worker_id}:`,
              infoTask
            );
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
    worker_id,
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
