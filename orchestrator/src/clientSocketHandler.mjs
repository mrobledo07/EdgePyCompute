// clients/socketHandler.mjs

import { taskClients, taskQueue } from "./state.mjs";

export function handleClientSocket(ws, taskId) {
  console.log(`🔌 Client connected for task ${taskId}`);

  if (!taskClients.has(taskId)) {
    taskClients.set(taskId, {});
  }
  taskClients.get(taskId).ws = ws;

  ws.on("close", () => {
    console.log(`❌ Client disconnected from task ${taskId}`);
    const client = taskClients.get(taskId);
    if (client?.numTasks > 0) {
      taskQueue = taskQueue.filter((task) => task.taskId !== taskId);
      console.log(
        `🗑️ Tasks ${taskId} removed from queue due to client disconnect.`
      );
    }
    taskClients.delete(taskId);
  });
}
