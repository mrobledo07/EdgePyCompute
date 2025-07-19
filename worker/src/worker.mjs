import axios from "axios";
import WebSocket from "ws";
import { executeTask } from "./taskExecutor.mjs";
import { HTTP_ORCH, WS_ORCH } from "./config.mjs";

export async function registerWorker() {
  try {
    const { data } = await axios.post(`${HTTP_ORCH}/register_worker`, {
      numWorkers: 1,
    });

    const workerId = data.worker_id;
    const ws = new WebSocket(`${WS_ORCH}?worker_id=${workerId}`);

    ws.on("open", () => console.log("🔌 Connected to orchestrator"));

    ws.on("message", async (msg) => {
      const task = JSON.parse(msg.toString());
      const { code, arg, taskId, type } = task;

      if (!code || !arg || !taskId || !type) {
        console.error(
          "❌ Invalid task received from ORCHESTRATOR. Missing code, arg, taskID, or type."
        );
        ws.send(
          JSON.stringify({
            arg: task.arg,
            taskId: task.taskId,
            status: "error",
            result:
              "Invalid task received from ORCHESTRATOR. Missing code, arg, taskID, or type.",
          })
        );
        return;
      }

      console.log(
        `▶️ Worker ${workerId} received task ${task.arg}:${task.taskId}`
      );
      await executeTask(task, ws);
    });

    ws.on("error", (err) => {
      console.error("❌ WebSocket ORCHESTRATOR error:", err.message);
    });
    ws.on("close", () =>
      console.log("🔌 WebSocket connection to ORCHESTRATOR closed.")
    );
  } catch (err) {
    if (err.response) {
      console.error(
        "❌ Error response from ORCHESTRATOR server:",
        err.response.data || err.message
      );
    } else if (err.request) {
      console.error(
        "❌ No response received from the ORCHESTRATOR server. The server may be down."
      );
    } else {
      console.error("❌ Error:", err.message);
    }
  }

  return { workerId, ws };
}
