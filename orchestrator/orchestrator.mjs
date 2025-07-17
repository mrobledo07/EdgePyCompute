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

// A map to hold possible mapreduce tasks with stages in order
const mapreduceTasks = new Map();

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
  while (
    taskQueue.length > 0 &&
    workers.length > 0 &&
    workers[0].availableWorkers > 0
  ) {
    const next = taskQueue[0]; // peek
    // Intentamos despachar SIN sacarla aún
    const canDispatch = (() => {
      const numWorkers = getWorkersAvailable().length;
      if (
        next.type === "mapreducewordcount" ||
        next.type === "mapreduceterasort"
      ) {
        console.log("TRYING TO DISPATCH MAPPERS");
        console.log("NUM MAPPERS:", next.arg[1]);
        console.log("AVAILABLE WORKERS:", numWorkers);
        return numWorkers >= next.arg[1];
      }
      if (next.type === "reduceterasort") {
        console.log("TRYING TO DISPATCH REDUCERS");
        console.log("NUM REDUCERS:", next.numReducers);
        console.log("AVAILABLE WORKERS:", numWorkers);
        return numWorkers >= next.numReducers;
      }
      return workers[0].availableWorkers > 0;
    })();

    console.log(
      `Processing queue. Tasks waiting: ${taskQueue.length}. Top worker availability: ${workers[0].availableWorkers}`
    );

    if (!canDispatch) {
      console.log(
        `🕒 No available workers. Job "${next.arg}:${next.taskId}" queued.`
      );
      break;
    } // No tenemos recursos → salimos del bucle

    // Podemos despachar: ahora sí shift y dispatch
    const task = taskQueue.shift();
    dispatchTask(task);
  }
};

const getWorkersAvailable = () => {
  let availWorkers = [];
  let numWorkers = 0;
  for (const worker of workers) {
    if (worker.availableWorkers == 0) return availWorkers;
    for (let i = 0; i < worker.availableWorkers; i++) {
      worker.worker_num = numWorkers;
      availWorkers.push(worker);
      numWorkers++;
    }
  }
  return availWorkers;
};

const dispatchMappers = (task) => {
  const availableWorkers = getWorkersAvailable();

  console.log("TRYING TO DISPATCH MAPPERS CODE,", task.code);
  let mapper_code = task.code[0];
  let reducer_code = task.code[1];
  console.log("MAPPER CODE:", mapper_code);
  console.log("REDUCER CODE:", reducer_code);
  const nummappers = task.arg[1];
  console.log("NUM MAPPERS:", nummappers);
  console.log("AVAILABLE WORKERS:", availableWorkers.length);
  if (nummappers > availableWorkers.length) {
    console.log(
      `🕒 No available workers. Job for MAPPERS "${task.arg}:${task.taskId}" queued.`
    );
    return false;
  } else {
    /* In our implementation, we do not have partitioning and shuffling for mapreduce wordcount
    so we restrict the number of reducers to 1 */
    if (task.type == "mapreducewordcount") {
      task.type = "mapwordcount";
      task.numReducers = 1;
    } else {
      const numReducers = task.arg[2];
      task.numReducers = numReducers;
      task.type = "mapterasort";
    }
    task.code = mapper_code; // Use the first code for mappers
    task.numMappers = nummappers;
    task.arg = task.arg[0];
    let i = 0;
    for (const worker of availableWorkers) {
      if (i == nummappers) break;
      task.numWorker = worker.worker_num;
      reserveWorkerAndSendTask(worker, task);
      i += 1;
    }
    const infoMapReduce = {
      numMappers: task.numMappers,
      numReducers: task.numReducers,
      codeReduce: reducer_code,
      type: task.type,
      results: [],
    };
    mapreduceTasks.set(task.taskId, infoMapReduce);
    return true;
  }
};

const dispatchReducers = (task) => {
  const availableWorkers = getWorkersAvailable();
  console.log("TRYING TO DISPATCH REDUCERS CODE,", task.code);
  const reducer_code = task.code;
  console.log("REDUCER CODE:", reducer_code);
  const numReducers = task.numReducers;
  if (numReducers > availableWorkers.length) {
    console.log(
      `🕒 No available workers. Job for REDUCERS TERASORT "${task.arg}:${task.taskId}" queued.`
    );
    return false;
  } else {
    const numMappers = task.numMappers;
    const args_reducer = [];

    for (let r = 0; r < numReducers; r++) {
      for (let m = 0; m < numMappers; m++) {
        args_reducer.push(task.arg[m][r]);
      }
    }

    let i = 0;
    for (const worker of availableWorkers) {
      if (i == numReducers) break;
      task.arg = args_reducer;
      task.numWorker = worker.worker_num;
      reserveWorkerAndSendTask(worker, task);
      i += 1;
    }
    return true;
  }
};

/**
 * Assigns a single task to the most available worker.
 * If no worker is available, the task is added to the queue.
 * @param {object} task - The task object { code, arg, taskId }.
 */
const dispatchTask = (task) => {
  if (task.type === "mapreducewordcount" || task.type === "mapreduceterasort") {
    const ok = dispatchMappers(task);
    if (!ok) taskQueue.push(task);
    return;
  }

  if (task.type === "reduceterasort") {
    const ok = dispatchReducers(task);
    if (!ok) taskQueue.push(task);
    return;
  }

  const worker = workers[0];

  if (worker && worker.availableWorkers > 0) {
    reserveWorkerAndSendTask(worker, task);
  } else {
    console.log(
      `🕒 No available workers. Task for "${task.arg}:${task.taskId}" queued.`
    );
    taskQueue.push(task);
  }
};

const reserveWorkerAndSendTask = (worker, task) => {
  // 1) Reserve the worker for this task
  worker.availableWorkers--;
  worker.tasksAssignated.push(task);
  sortWorkers(); // Re-sort after reserving a worker

  // 2) Send the task to the worker
  worker.ws.send(JSON.stringify(task), (err) => {
    if (err) {
      console.error(
        `❌ Error sending task to worker ${worker.worker_id}:`,
        err.message
      );
      // 3) If sending fails, re-queue the task
      worker.availableWorkers++;
      worker.tasksAssignated = worker.tasksAssignated.filter(
        (t) => t.taskId !== task.taskId && t.arg !== task.arg
      );
      taskQueue.unshift(task);
      sortWorkers(); // Re-sort to reflect new availability
    } else {
      console.log(
        `✅ Task ${task.arg}:${task.taskId} sent to worker ${worker.worker_id}`
      );
    }
  });
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
      let numTasks = taskClients.get(taskId).numTasks;
      taskClients.delete(taskId);
      if (numTasks > 0) {
        taskQueue = taskQueue.filter((task) => task.taskId !== taskId);
        console.log(
          `🗑️ Tasks ${taskId} removed from queue due to client disconnect.`
        );
      }
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
      // If worker had tasks assigned, they need to be re-queued
      const worker = workers.find((w) => w.worker_id === workerId);
      if (worker.tasksAssignated.length > 0) {
        taskQueue.push(...worker.tasksAssignated);
        console.log(
          "Tasks re-queued from disconnected worker:",
          worker.tasksAssignated.map((task) => `${task.arg}:${task.taskId}`)
        );
      }

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
        worker.tasksAssignated = worker.tasksAssignated.filter(
          (t) => t.taskId !== msg.taskId && t.arg !== msg.arg
        ); // Remove the completed task from the worker's list

        console.log(
          `✨ Worker ${workerId} now has ${worker.availableWorkers} available processors.`
        );
        sortWorkers(); // Re-sort to reflect new availability
      }

      // If it is a mapreduce task, decrease counter and do not send result to client
      let infoTask = mapreduceTasks.get(msg.taskId);
      if (infoTask && infoTask.numMappers > 0) {
        console.log("CODE REDUCE:", infoTask.codeReduce);
        infoTask.numMappers--;
        infoTask.results.push(msg.result);
        mapreduceTasks.set(msg.taskId, infoTask);
        console.log(
          `📦 Worker ${workerId} completed a mapper for task ${msg.taskId}.
          Remaining mappers: ${infoTask.numMappers}`
        );
      }

      if (infoTask && infoTask.numMappers == 0) {
        // If map stage of this job ended, we can start reduce
        console.log("CODE REDUCE:", infoTask.codeReduce);

        console.log(
          `🔄 Map stage for task ${msg.taskId} completed. Starting reduce stage.`
        );
        //
        let taskId = msg.taskId;
        let numReducers = infoTask.numReducers;
        let numMappers = infoTask.numMappers;
        let type;
        if (infoTask.type === "mapwordcount") {
          type = "reducewordcount";
        } else {
          type = "reduceterasort";
        }

        infoTask.numMappers = -1; // Mark mappers as completed
        mapreduceTasks.set(taskId, infoTask);
        let reduceTask = {
          code: infoTask.codeReduce,
          arg: infoTask.results,
          taskId,
          type,
          numReducers,
          numMappers,
        };
        console.log(
          `📦 Dispatching reduce task for ${taskId} with results ${infoTask.results} and code ${infoTask.codeReduce}`
        );
        dispatchTask(reduceTask);
      }

      if (infoTask && infoTask.numMappers == -1) {
        infoTask.numReducers--;
        if (infoTask.numReducers == 0) {
          mapreduceTasks.delete(msg.taskId);
          console.log(
            `✅ All reducers for task ${msg.taskId} completed. Task finished.`
          );
        } else {
          console.log(
            `📦 Worker ${workerId} completed a reducer for task ${msg.taskId}.
            Remaining reducers: ${infoTask.numReducers}`
          );
        }
      }

      // Forward result to the client
      if (!infoTask) {
        console.log("📤 Forwarding result to client:", msg);
        const clientInfo = taskClients.get(msg.taskId);
        if (clientInfo && clientInfo.ws) {
          clientInfo.ws.send(
            JSON.stringify({
              arg: msg.arg,
              status: msg.status,
              result: msg.result,
            })
          );
          clientInfo.numTasks--; // Decrease the number of tasks for this client
          if (clientInfo.numTasks <= 0) {
            console.log(`✅ All tasks for client ${msg.taskId} completed.`);
            clientInfo.ws.close(); // Close the WebSocket connection
          }
        } else {
          console.error(
            `❌ Client for task ID ${msg.taskId} not found or disconnected.`
          );
        }
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
    tasksAssignated: [],
  };

  workers.push(newWorker);
  sortWorkers(); // Sort the list after adding a new worker

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
    const individualTask = { code, arg, taskId, type };
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
