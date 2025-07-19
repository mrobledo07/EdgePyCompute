// workers/socketHandler.mjs
// import { workers } from "./state.mjs";
// import { sortWorkers } from "./workerManager.mjs";
import { taskClients, mapreduceTasks } from "./state.mjs";
import { processTaskQueue, dispatchTask } from "./tasksDispatcher.mjs";
import workerRegistry from "./workerRegistry.mjs";
import taskQueue from "./taskQueue.mjs";

export function handleWorkerSocket(ws, workerId) {
  console.log(`🔌 Worker connected with ID ${workerId}`);

  // const worker = workers.find((w) => w.worker_id === workerId);
  const worker = workerRegistry.getWorkerById(workerId);
  worker.ws = ws;
  processTaskQueue();

  ws.on("close", () => {
    console.log(`❌ Worker disconnected with ID ${workerId}`);
    // const worker = workers.find((w) => w.worker_id === workerId);
    const worker = workerRegistry.getWorkerById(workerId);
    if (worker && worker?.tasksAssignated.length > 0) {
      taskQueue.push([...worker.tasksAssignated.values()]);
      console.log(
        "Tasks re-queued from disconnected worker:",
        worker.tasksAssignated.map((t) => `${t.arg}:${t.taskId}`)
      );
    }
    workerRegistry.removeWorker(workerId);
    //workers = workers.filter((w) => w.worker_id !== workerId);
    //sortWorkers();
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
    //const worker = workers.find((w) => w.worker_id === workerId);
    const worker = workerRegistry.getWorkerById(workerId);

    const completed = workerRegistry.completeTaskOnWorker(workerId, msg.taskId);
    if (completed) {
      console.log(
        `✨ Worker ${workerId} now has ${worker.availableWorkers} available processors.`
      );
    } else {
      console.error(
        `❌ Worker ${workerId} not found or task completion failed.`
      );
      return;
    }

    let infoTask = mapreduceTasks.get(msg.taskId);
    if (infoTask && infoTask.numMappers > 0) {
      infoTask.numMappers--;
      infoTask.results.push(msg.result);
      mapreduceTasks.set(msg.taskId, infoTask);
      console.log(
        `📦 Worker ${workerId} completed a mapper for task ${msg.taskId}. Remaining: ${infoTask.numMappers}`
      );
    }

    if (infoTask && infoTask.numMappers === 0) {
      infoTask.numMappers = -1;
      mapreduceTasks.set(msg.taskId, infoTask);

      const type =
        infoTask.type === "mapwordcount" ? "reducewordcount" : "reduceterasort";

      const reduceTask = {
        code: infoTask.codeReduce,
        arg: infoTask.results,
        taskId: msg.taskId,
        type,
        numReducers: infoTask.numReducers,
        numMappers: infoTask.numMappers,
      };

      console.log(
        `🔄 Map stage completed for ${msg.taskId}. Starting reduce phase.`
      );
      dispatchTask(reduceTask);
    }

    if (infoTask && infoTask.numMappers === -1) {
      infoTask.numReducers--;
      if (infoTask.numReducers === 0) {
        mapreduceTasks.delete(msg.taskId);
        console.log(`✅ All reducers for task ${msg.taskId} completed.`);
      } else {
        console.log(
          `📦 Reducer completed for ${msg.taskId}. Remaining: ${infoTask.numReducers}`
        );
      }
    }

    if (!infoTask) {
      const clientInfo = taskClients.get(msg.taskId);
      if (clientInfo?.ws) {
        clientInfo.ws.send(
          JSON.stringify({
            message_type: "task_result",
            arg: msg.arg,
            status: msg.status,
            result: msg.result,
          })
        );
        clientInfo.numTasks--;
        if (clientInfo.numTasks <= 0) {
          console.log(`✅ All tasks for client ${msg.taskId} completed.`);
          clientInfo.ws.close();
        }
      } else {
        console.error(
          `❌ Client for task ${msg.taskId} not found or disconnected.`
        );
      }
    }

    processTaskQueue();
  });
}
