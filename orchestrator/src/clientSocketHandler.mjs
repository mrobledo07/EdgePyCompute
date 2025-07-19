// clients/socketHandler.mjs

import { taskClients } from "./state.mjs";
import taskQueue from "./taskQueue.mjs";

export function handleClientSocket(ws, taskId) {
  console.log(`🔌 Client connected for task ${taskId}`);

  taskClients.get(taskId).ws = ws;

  ws.on("close", () => {
    console.log(`❌ Client disconnected from task ${taskId}`);
    const client = taskClients.get(taskId);
    if (client?.numTasks > 0) {
      //taskQueue = taskQueue.filter((task) => task.taskId !== taskId);
      taskQueue.remove(taskId);
      console.log(
        `🗑️ Tasks ${taskId} removed from queue due to client disconnect.`
      );
    }
    taskClients.delete(taskId);
  });
}
