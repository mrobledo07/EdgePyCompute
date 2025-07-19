// tasks/dispatcher.mjs

import { mapreduceTasks } from "./state.mjs";
import workerRegistry from "./workerRegistry.mjs";
import taskQueue from "./taskQueue.mjs";

/**
 * Processes the task queue by trying to assign tasks to available workers.
 */
export function processTaskQueue() {
  while (
    taskQueue.size() > 0 &&
    workerRegistry.getTotalAvailableWorkers() > 0
  ) {
    const next = taskQueue.peek(); // Peek
    const numAvailableWorkers = workerRegistry.getTotalAvailableWorkers();

    const canDispatch = (() => {
      if (
        next.type === "mapreducewordcount" ||
        next.type === "mapreduceterasort"
      ) {
        return numAvailableWorkers >= next.arg[1]; // num mappers
      }

      if (next.type === "reduceterasort") {
        return numAvailableWorkers >= next.numReducers;
      }

      return numAvailableWorkers > 0;
    })();

    if (!canDispatch) {
      console.log(
        `🕒 No available workers. Task "${next.arg}:${next.taskId}" remains in queue.`
      );
      break;
    }

    const task = taskQueue.shift(); // Remove from queue
    dispatchTask(task);
  }
}

/**
 * Dispatches a single task depending on its type.
 */
export function dispatchTask(task) {
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

  // Default case: normal task
  const worker = workerRegistry.getBestWorkers(1)[0];
  if (worker) {
    task.code = task.code[0]; // Use first code block
    reserveWorkerAndSendTask(worker, task);
  } else {
    console.log(
      `🕒 No available workers. Queuing task "${task.arg}:${task.taskId}"`
    );
    taskQueue.push(task);
  }
}

/**
 * Dispatches map tasks to available workers.
 */
function dispatchMappers(task) {
  const numAvailableWorkers = workerRegistry.getTotalAvailableWorkers();
  const [mapperCode, reducerCode] = task.code;
  const numMappers = task.arg[1];

  if (numMappers > numAvailableWorkers) {
    console.log(`🕒 Not enough workers for map phase of ${task.taskId}`);
    return false;
  }

  // Prepare mapper task
  task.code = mapperCode;
  task.numMappers = numMappers;

  if (task.type === "mapreducewordcount") {
    task.type = "mapwordcount";
    task.numReducers = 1;
  } else {
    const numReducers = task.arg[2];
    task.type = "mapterasort";
    task.numReducers = numReducers;
  }

  task.arg = task.arg[0]; // raw args to send to each mapper
  const workersAvailable = workerRegistry.getBestWorkers(numMappers);
  // Dispatch to N mappers
  for (let i = 0; i < numMappers; i++) {
    const worker = workersAvailable[i];
    const individualTask = { ...task, numWorker: worker.worker_num };
    reserveWorkerAndSendTask(worker, individualTask);
  }

  mapreduceTasks.set(task.taskId, {
    numMappers: task.numMappers,
    numReducers: task.numReducers,
    codeReduce: reducerCode,
    type: task.type,
    results: [],
  });

  return true;
}

/**
 * Dispatches reduce tasks to available workers.
 */
function dispatchReducers(task) {
  const numAvailableWorkers = workerRegistry.getTotalAvailableWorkers();
  const numReducers = task.numReducers;

  if (numReducers > numAvailableWorkers) {
    console.log(`🕒 Not enough workers for reduce phase of ${task.taskId}`);
    return false;
  }
  // task.arg is in the format [ [ “…_0.txt”, “…_1.txt”, … ],  // mapper 0
  //                         [ “…_0.txt”, “…_1.txt”, … ],     // mapper 1
  //                         …                          ]    // mapper m

  const argsMatrix = task.arg; // [ [m0_files], [m1_files], ... ]
  const workersAvailable = workerRegistry.getBestWorkers(numReducers);

  for (let r = 0; r < numReducers; r++) {
    const reducerArgs = [];

    for (const mapperFiles of argsMatrix) {
      const file = mapperFiles.find((f) => f.endsWith(`_${r}.txt`));
      if (!file) {
        throw new Error(`Reducer ${r} missing input from mapper.`);
      }
      reducerArgs.push(file);
    }

    const reducerTask = {
      ...task,
      arg: reducerArgs,
      numWorker: workersAvailable[r].worker_num,
    };

    reserveWorkerAndSendTask(workersAvailable[r], reducerTask);
  }

  return true;
}

/**
 * Assigns a task to a worker and sends it via WebSocket.
 */
function reserveWorkerAndSendTask(worker, task) {
  //worker.availableWorkers--;
  //worker.tasksAssignated.push(task);
  //sortWorkers();
  workerRegistry.assignTaskToWorker(worker.worker_id, task.taskId, task);

  worker.ws.send(JSON.stringify(task), (err) => {
    if (err) {
      console.error(
        `❌ Failed to send task to ${worker.worker_id}:`,
        err.message
      );
      // Recover worker and re-queue task
      // worker.availableWorkers++;
      // worker.tasksAssignated = worker.tasksAssignated.filter(
      //   (t) => t.taskId !== task.taskId || t.arg !== task.arg
      // );
      // taskQueue.unshift(task);
      // sortWorkers();
      workerRegistry.completeTaskOnWorker(worker.worker_id, task.taskId);
    } else {
      console.log(
        `✅ Sent task ${task.taskId}:${task.arg} to worker ${worker.worker_id}`
      );
    }
  });
}
