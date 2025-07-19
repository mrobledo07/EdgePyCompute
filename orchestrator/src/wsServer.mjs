// wsServer.mjs

import { WebSocketServer } from "ws";
import { handleClientSocket } from "./clientSocketHandler.mjs";
import { handleWorkerSocket } from "./workerSocketHandler.mjs";

export const wss = new WebSocketServer({ noServer: true });

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const taskId = url.searchParams.get("task_id");
  const workerId = url.searchParams.get("worker_id");

  if (taskId) {
    handleClientSocket(ws, taskId);
  } else if (workerId) {
    handleWorkerSocket(ws, workerId);
  } else {
    console.error("❌ WebSocket connection without task_id or worker_id");
    ws.close();
  }
});
