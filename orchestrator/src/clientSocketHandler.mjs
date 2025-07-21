// clients/socketHandler.mjs

// import { taskClients } from "./state.mjs";
//import taskQueue from "./taskQueue.mjs";
import clientRegistry from "./clientRegistry.mjs";
import taskQueue from "./taskQueue.mjs";

export function handleClientSocket(ws, clientId) {
  console.log(`🔌 Client connected ${clientId}`);

  //taskClients.get(taskId).ws = ws;
  //const client = (clientRegistry.getClient(clientId).ws = ws);
  clientRegistry.getClient(clientId).ws = ws;

  ws.on("close", () => {
    console.log(`❌ Client disconnected ${clientId}`);
    // if (client?.numTasks > 0) {
    //   //taskQueue = taskQueue.filter((task) => task.taskId !== taskId);
    //   const clientTasks = clientRegistry.getClientTasks(clientId);
    //   const taskIds = clientTasks.map((task) => task.taskId);
    //   taskQueue.remove(taskIds);
    //   console.log(
    //     `🗑️ Tasks ${taskIds} removed from queue due to client disconnect.`
    //   );
    // }
    // clientRegistry.removeClient(clientId); // Remove client from registry
    if (clientRegistry.allTasksExecuted(clientId)) {
      const clientTasks = clientRegistry.getClientTasks(clientId);
      const taskIds = clientTasks.map((task) => task.taskId);
      taskQueue.removeClientTasks(taskIds); // remove in case tasks are still pending
      clientRegistry.removeClient(clientId);
    }
    // Remove client from registry if all tasks executed
    // else
    //   console.log(
    //     `⚠️ Client ${clientId} disconnected but tasks still pending.`
    //   );
  });
}
