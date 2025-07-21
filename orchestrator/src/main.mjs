import express from "express";
import { v4 as uuidv4 } from "uuid";
import { wss } from "./wsServer.mjs";
//import { workers } from "./state.mjs";
//import { sortWorkers } from "./workerManager.mjs";
import { dispatchTask } from "./tasksDispatcher.mjs";
// import { taskClients } from "./state.mjs";
import workerRegistry from "./workerRegistry.mjs";
import clientRegistry from "./clientRegistry.mjs";
// import { Stopwatch } from "./stopWatch.mjs";

const app = express();
const port = 3000;

app.use(express.json());

// --- HTTP Endpoints ---

// app.post("/client_connect", (req, res) => {
//   const clientId = req.body.client_id;

//   if (!clientId || !clientRegistry.has(clientId)) {
//     return res.status(404).json({ error: "Client not found" });
//   }

//   return res.status(200).json({ client_id: clientId });
// });

app.post("/register_worker", (req, res) => {
  const worker_id = uuidv4();
  const { numWorkers } = req.body;

  const newWorker = {
    worker_id,
    ws: null, // Will be set upon WebSocket connection
    maxWorkers: numWorkers || 1,
    availableWorkers: numWorkers || 1,
    tasksAssignated: [],
  };

  //workers.push(newWorker);
  //sortWorkers();
  workerRegistry.addWorker(newWorker);
  console.log(
    `👍 Worker registered with ID: ${worker_id} and ${newWorker.availableWorkers} processors.`
  );

  res.json({ message: "Worker registered successfully", worker_id });
});

app.post("/register_task", (req, res) => {
  const { code, args, type } = req.body;

  if (
    !Array.isArray(code) ||
    code.length === 0 ||
    !Array.isArray(args) ||
    args.length === 0 ||
    !type
  ) {
    return res.status(400).json({ error: "Missing code, arguments or type." });
  }

  const clientId = uuidv4();
  // const newTask = {
  //   numTasks: args.length,
  //   ws: null,
  // };

  //taskClients.set(taskId, newTask);
  clientRegistry.registerClient(clientId, null, 0);
  console.log(
    ">> clients map keys after registering client:",
    Array.from(clientRegistry.clients.keys())
  );

  console.log(
    `📦 Registering client ${clientId} with ${args.length} tasks of type ${type}.`
  );
  console.log(
    `✅ Client registered with ID: ${clientId} for ${args.length} tasks.`
  );

  res.json({ message: "Client registered successfully", client_id: clientId });
  // Dispatch each individual task
  for (const arg of args) {
    //const sw = new Stopwatch();
    const taskId = uuidv4();
    clientRegistry.addTask(clientId, {
      code,
      arg,
      taskId,
      type,
      //stopwatch: sw,
      executionTime: 0,
    });
    // console.log(
    //   ">> clients map keys after adding tasks:",
    //   Array.from(clientRegistry.clients.keys())
    // );
    console.log(
      `📨 Dispatching task ${taskId} for client ${clientId} with arg: ${arg}`
    );
    dispatchTask({ clientId, taskId, code, arg, type });
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
