import express from "express";
import { WebSocketServer } from "ws";
import { v4 as uuidv4 } from "uuid";

const app = express();
const port = 3000;

app.use(express.json());

// --- Data Structures ---

// Map to store client connections by their task ID.
const taskClients = new Map();

// An array to hold worker objects. This will be kept sorted.
let workers = [];

// A queue for tasks that are waiting for an available worker.
let taskQueue = [];

// --- Helper Functions ---

/**
 * Sorts the workers array in place.
 * Workers with more available processors are moved to the front.
 */
const sortWorkers = () => {
  workers.sort((a, b) => b.availableWorkers - a.availableWorkers);
  // console.log('Workers sorted. Current availability:', workers.map(w => `${w.worker_id.slice(0, 8)}: ${w.availableWorkers}`).join(', '));
};

/**
 * Tries to assign tasks from the queue to any available workers.
 */
const processTaskQueue = () => {
  // Keep processing while there are tasks and available workers
  while (
    taskQueue.length > 0 &&
    workers.length > 0 &&
    workers[0].availableWorkers > 0
  ) {
    console.log(
      `Processing queue. Tasks waiting: ${taskQueue.length}. Top worker availability: ${workers[0].availableWorkers}`
    );
    // Dequeue the oldest task
    const task = taskQueue.shift();
    // Dispatch it
    dispatchTask(task);
  }
};

/**
 * Assigns a single task to the most available worker.
 * If no worker is available, the task is added to the queue.
 * @param {object} task - The task object { code, arg, taskId }.
 */
const dispatchTask = (task) => {
  // Find the most available worker (always the first in the sorted array)
  const mostAvailableWorker = workers[0];

  if (mostAvailableWorker && mostAvailableWorker.availableWorkers > 0) {
    // A worker is available, send the task
    mostAvailableWorker.ws.send(JSON.stringify(task), (err) => {
      if (err) {
        console.error(
          `❌ Error sending task to worker ${mostAvailableWorker.worker_id}:`,
          err.message
        );
        // If sending fails, put the task back at the front of the queue
        taskQueue.unshift(task);
      } else {
        console.log(
          `✅ Task ${arg}:${task.taskId} sent to worker ${mostAvailableWorker.worker_id}`
        );
        mostAvailableWorker.availableWorkers--;
        // Re-sort the list since a worker's availability has changed
        sortWorkers();
      }
    });
  } else {
    // No workers available, queue the task
    console.log(
      `🕒 No available workers. Task for "${task.arg}:${task.taskId}" queued.`
    );
    taskQueue.push(task);
  }
};

// --- WebSocket Server ---

const wss = new WebSocketServer({ noServer: true });

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const taskId = url.searchParams.get("task_id");
  const workerId = url.searchParams.get("worker_id");

  if (taskId) {
    // Handle client connection
    console.log(`🔌 Client connected for task ${taskId}`);
    // A client might connect before the task is fully registered, so ensure an entry exists.
    if (!taskClients.has(taskId)) {
      taskClients.set(taskId, {});
    }
    taskClients.get(taskId).ws = ws;

    ws.on("close", () => {
      console.log(`❌ Client disconnected from task ${taskId}`);
      taskClients.delete(taskId);
    });
  } else if (workerId) {
    // Handle worker connection
    console.log(`🔌 Worker connected with ID ${workerId}`);
    const worker = workers.find((w) => w.worker_id === workerId);
    if (worker) {
      worker.ws = ws; // Attach WebSocket connection to the worker object
      // A worker connecting might mean it can take on queued tasks
      processTaskQueue();
    }

    ws.on("close", () => {
      console.log(`❌ Worker disconnected with ID ${workerId}`);
      // Remove the worker from the list
      workers = workers.filter((w) => w.worker_id !== workerId);
      sortWorkers(); // Re-sort the list
    });

    ws.on("message", (data) => {
      let msg;
      try {
        msg = JSON.parse(data.toString());
      } catch (e) {
        console.error("❌ Invalid JSON from worker:", data.toString());
        return;
      }

      console.log(`📨 Message from worker ${workerId}:`, msg);

      const worker = workers.find((w) => w.worker_id === workerId);
      if (worker) {
        // A worker finished a task, so its availability increases
        worker.availableWorkers++;
        console.log(
          `✨ Worker ${workerId} now has ${worker.availableWorkers} available processors.`
        );
        sortWorkers(); // Re-sort to reflect new availability
      }

      // Forward result to the client
      const clientInfo = taskClients.get(msg.taskId);
      if (clientInfo && clientInfo.ws) {
        clientInfo.ws.send(
          JSON.stringify({
            arg: msg.arg,
            status: msg.status,
            result: msg.result,
          })
        );
      } else {
        console.error(
          `❌ Client for task ID ${msg.taskId} not found or disconnected.`
        );
      }

      // Since a worker is now free, check the queue for pending tasks
      processTaskQueue();
    });
  } else {
    console.error("❌ WebSocket connection without task_id or worker_id");
    ws.close();
  }
});

// --- API Endpoints ---

app.post("/register_worker", (req, res) => {
  const worker_id = uuidv4();
  const { numWorkers } = req.body;

  const newWorker = {
    worker_id,
    ws: null, // WebSocket will be attached on connection
    maxWorkers: numWorkers || 1,
    availableWorkers: numWorkers || 1,
  };

  workers.push(newWorker);
  sortWorkers(); // Sort the list after adding a new worker

  console.log(
    `👍 Worker registered with ID: ${worker_id} and ${newWorker.availableWorkers} processors.`
  );
  res.json({ message: "Worker registered successfully", worker_id });
});

app.post("/register_task", (req, res) => {
  const { code, args } = req.body;

  if (!code || !Array.isArray(args) || args.length === 0) {
    return res.status(400).json({ error: "Missing code or arguments." });
  }

  const newTask = {
    numTasks: args.length,
    ws: null, // WebSocket will be attached on connection
  };

  const taskId = uuidv4();
  taskClients.set(taskId, newTask);

  console.log(
    `✅ Task registered with ID: ${taskId} for ${args.length} sub-tasks.`
  );
  res.json({ message: "Task registered successfully", task_id: taskId });

  // For each argument, create and dispatch a task
  for (const arg of args) {
    const individualTask = { code, arg, taskId };
    dispatchTask(individualTask);
  }
});

// --- Server Startup ---
const server = app.listen(port, () => {
  console.log(`🚀 Orchestrator listening at http://localhost:${port}`);
});

server.on("upgrade", (req, socket, head) => {
  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit("connection", ws, req);
  });
});
