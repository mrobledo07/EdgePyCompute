// workers/socketHandler.mjs
// import { workers } from "./state.mjs";
// import { sortWorkers } from "./workerManager.mjs";
import { mapreduceTasks } from "./tasksMapReduce.mjs";
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
    const cleanedTaskId = msg.taskId.replace(/-(mapper\d+|reducer\d*?)$/, "");

    const clientInfo = clientRegistry.getClient(msg.clientId);
    if (!clientInfo) {
      console.warn(
        `⚠️ Tried to access task of non-existent client ${msg.clientId}`
      );
      return;
    }
    const clientTask = clientRegistry.getClientTask(
      msg.clientId,
      cleanedTaskId
    );
    if (!clientTask) {
      console.error(
        `❌ Task ${cleanedTaskId} for client ${msg.clientId} not found in registry.`
      );
      return;
    }

    if (clientTask.state === "done" || clientTask.state === "error") {
      console.warn(
        `⚠️ Task ${cleanedTaskId} for client ${msg.clientId} already marked as ${clientTask.state}. Ignoring message.`
      );
      return;
    }

    // console.log(
    //   ">> task COMPLETED clients map keys:",
    //   Array.from(clientRegistry.clients.keys())
    // );

    let infoTask = mapreduceTasks.get(cleanedTaskId);

    if (infoTask && msg.status === "error")
      mapreduceTasks.delete(cleanedTaskId); // Mapreduce task failed if any mapper or reducer fails

    infoTask = mapreduceTasks.get(cleanedTaskId);

    if (infoTask && infoTask.numMappers > 0) {
      infoTask.numMappers--;
      infoTask.results.push(msg.result);
      //mapreduceTasks.set(cleanedTaskId, infoTask);
      console.log(
        `📦 Worker ${workerId} completed a mapper for task ${cleanedTaskId}. Remaining: ${infoTask.numMappers}`
      );
    }

    // console.log(
    //   ">> task COMPLETED after infoTask clients map keys:",
    //   Array.from(clientRegistry.clients.keys())
    // );

    //infoTask = mapreduceTasks.get(msg.taskId);
    if (infoTask && infoTask.numMappers === 0) {
      infoTask.numMappers = -1;
      //mapreduceTasks.set(msg.taskId, infoTask);

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
        taskId: cleanedTaskId,
        clientId: msg.clientId,
        type,
        numReducers: infoTask.numReducers,
        numMappers: infoTask.numMappers,
      };

      const clientTask = clientRegistry.getClientTask(
        msg.clientId,
        cleanedTaskId
      );
      clientTask.stopwatch.stop(); // Stop stopwatch for the task
      clientTask.executionTime += clientTask.stopwatch.getDuration().toFixed(2);
      console.log(
        `🔄 Map stage completed for ${cleanedTaskId}. Starting reduce phase.`
      );
      const dispatched = dispatchTask(reduceTask);
      if (!dispatched) {
        mapreduceTasks.delete(cleanedTaskId);
        msg.status = "error";
        msg.result = `Failed to dispatch reduce task for ${cleanedTaskId}. Task removed.`;
        console.error(
          `❌ Failed to dispatch reduce task for ${cleanedTaskId}. Task removed.`
        );
      } else {
        return; // Exit early if reduce task was dispatched successfully
      }
    }

    // console.log(
    //   ">> task COMPLETED clients map keys after map stage:",
    //   Array.from(clientRegistry.clients.keys())
    // );

    // console.log("!! tasks in mapreduceTasks:", mapreduceTasks.size);
    // console.log(">> mapreducetasks:", Array.from(mapreduceTasks.keys()));

    infoTask = mapreduceTasks.get(cleanedTaskId);
    //infoTask = mapreduceTasks.get(cleanedTaskId);
    if (infoTask && infoTask.numMappers === -1) {
      infoTask.numReducers--;
      if (infoTask.numReducers === 0) {
        const clientTask = clientRegistry.getClientTask(
          msg.clientId,
          cleanedTaskId
        );
        clientTask.stopwatch.stop(); // Stop stopwatch for the task
        clientTask.executionTime += clientTask.stopwatch
          .getDuration()
          .toFixed(2);
        mapreduceTasks.delete(cleanedTaskId);
        console.log(`✅ All reducers for task ${cleanedTaskId} completed.`);
      } else {
        console.log(
          `📦 Reducer completed for ${cleanedTaskId}. Remaining: ${infoTask.numReducers}`
        );
      }
    }

    // console.log(
    //   ">> task COMPLETED clients map keys before final:",
    //   Array.from(clientRegistry.clients.keys())
    // );
    // console.log("!! tasks in mapreduceTasks:", mapreduceTasks.size);
    // console.log(">> mapreducetasks:", Array.from(mapreduceTasks.keys()));

    infoTask = mapreduceTasks.get(cleanedTaskId);
    if (!infoTask) {
      if (msg.status === "done") {
        console.log(`✅ Task ${msg.taskId} completed by worker ${workerId}.`);
        console.log("🔍 Orchestrator got from worker:", msg);
        // console.log(
        //   ">> clients map keys in markdone:",
        //   Array.from(clientRegistry.clients.keys())
        // );
        clientRegistry.markTaskDone(msg.clientId, msg.taskId);
      } else if (msg.status === "error") {
        console.error(
          `❌ Error in task ${msg.taskId} from worker ${workerId}: ${msg.result}`
        );
        clientRegistry.markTaskError(msg.clientId, cleanedTaskId, msg.result);
      }

      // console.log(
      //   ">> task COMPLETED clients map keys final:",
      //   Array.from(clientRegistry.clients.keys())
      // );
      // console.log("!! tasks in mapreduceTasks:", mapreduceTasks.size);
      // console.log(">> mapreducetasks:", Array.from(mapreduceTasks.keys()));
      // console.log("!! tasks in mapreduceTasks:", mapreduceTasks.size);
      // console.log(">> mapreducetasks:", Array.from(mapreduceTasks.keys()));
      //const clientInfo = taskClients.get(msg.taskId);
      const clientInfo = clientRegistry.getClient(msg.clientId);
      const clientTask = clientRegistry.getClientTask(
        msg.clientId,
        cleanedTaskId
      );
      if (clientInfo?.ws) {
        clientInfo.ws.send(
          JSON.stringify({
            message_type: "task_result",
            arg: msg.arg,
            taskId: cleanedTaskId,
            status: msg.status,
            result: msg.result,
            executionTime: clientTask.executionTime,
          })
        );
        console.log(`📦 Before sending: numTasks = ${clientInfo.numTasks}`);

        clientInfo.numTasks--;
        console.log(
          `📦 Sent result for task ${cleanedTaskId} to client ${msg.clientId}. Remaining tasks: ${clientInfo.numTasks}`
        );
        if (clientInfo.numTasks <= 0) {
          clientRegistry.removeClient(msg.clientId);

          console.log(`✅ All tasks for client ${msg.clientId} completed.`);
          console.log(`🗑️ Client ${msg.clientId} removed from registry.`);

          clientInfo.ws.close();
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
