// workers/socketHandler.mjs
// import { workers } from "./state.mjs";
// import { sortWorkers } from "./workerManager.mjs";
import { mapreduceTasks } from "./state.mjs";
import { processTaskQueue, dispatchTask } from "./tasksDispatcher.mjs";
import workerRegistry from "./workerRegistry.mjs";
import taskQueue from "./taskQueue.mjs";
import clientRegistry from "./clientRegistry.mjs";

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
    if (worker && worker?.tasksAssignated.len > 0) {
      const tasksworker = [...worker.tasksAssignated.map.values()];
      taskQueue.push(tasksworker);
      console.log(
        "Tasks re-queued from disconnected worker:",
        tasksworker.map((t) => `${t.arg}:${t.taskId}`)
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

    infoTask = mapreduceTasks.get(msg.taskId);
    if (infoTask && infoTask.numMappers === 0) {
      infoTask.numMappers = -1;
      mapreduceTasks.set(msg.taskId, infoTask);

      const type =
        infoTask.type === "mapwordcount" ? "reducewordcount" : "reduceterasort";

      // clientRegistry.addTask(msg.clientId, {
      //   code: infoTask.codeReduce,
      //   arg: infoTask.results,
      //   taskId: msg.taskId,
      //   type,
      //   numReducers: infoTask.numReducers,
      //   numMappers: infoTask.numMappers,
      // });

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

    infoTask = mapreduceTasks.get(msg.taskId);
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

    if (!mapreduceTasks.has(msg.taskId)) {
      if (msg.status === "done") {
        console.log(`✅ Task ${msg.taskId} completed by worker ${workerId}.`);
        console.log("🔍 Orchestrator got from worker:", msg);
        console.log(
          ">> clients map keys in markdone:",
          Array.from(clientRegistry.clients.keys())
        );
        clientRegistry.markTaskDone(msg.clientId, msg.taskId);
      } else if (msg.status === "error") {
        console.error(
          `❌ Error in task ${msg.taskId} from worker ${workerId}: ${msg.result}`
        );
        clientRegistry.markTaskError(msg.clientId, msg.taskId, msg.result);
      }
      //const clientInfo = taskClients.get(msg.taskId);
      const clientInfo = clientRegistry.getClient(msg.clientId);
      if (clientInfo?.ws) {
        clientInfo.ws.send(
          JSON.stringify({
            message_type: "task_result",
            arg: msg.arg,
            taskId: msg.taskId,
            status: msg.status,
            result: msg.result,
          })
        );
        clientInfo.numTasks--;
        if (clientInfo.numTasks <= 0) {
          console.log(`✅ All tasks for client ${msg.clientId} completed.`);
          clientInfo.ws.close();
          clientRegistry.removeClient(msg.clientId);
          console.log(`🗑️ Client ${msg.clientId} removed from registry.`);
        }
      } else {
        console.error(
          `❌ Client ${msg.clientId} for task ${msg.taskId} not found or disconnected.`
        );
      }
    }

    processTaskQueue();
  });
}
