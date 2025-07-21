import axios from "axios";
import WebSocket from "ws";
import { executeTask } from "./taskExecutor.mjs";
import { HTTP_ORCH, WS_ORCH } from "./config.mjs";
import { Stopwatch } from "./stopWatch.mjs";

let workerId;
let ws;
let stopWatch;

let retryAttempts = 0;
const MAX_RETRIES = 3;

export async function registerWorker() {
  try {
    const { data } = await axios.post(`${HTTP_ORCH}/register_worker`, {
      numWorkers: 1,
    });
    workerId = data.worker_id;
    ws = new WebSocket(`${WS_ORCH}?worker_id=${workerId}`);
    stopWatch = new Stopwatch();
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

      retryAttempts++;
      if (retryAttempts > MAX_RETRIES) {
        console.error("❌ Maximum retry attempts reached. Exiting.");
        return;
      }

      // Retry after 3 seconds
      let countdown = 3;
      const interval = setInterval(() => {
        console.log(`🔁 Retrying in ${countdown}...`);
        countdown--;

        if (countdown < 0) {
          clearInterval(interval);
          registerWorker(); // Retry after countdown ends
        }
      }, 1000);
    } else {
      console.error("❌ Error:", err.message);
    }
    return;
  }

  ws.on("open", () => console.log("🔌 Connected to orchestrator"));

  ws.on("message", async (msg) => {
    try {
      const task = JSON.parse(msg.toString());
      const { clientId, taskId, code, arg, type } = task;

      if (!clientId || !taskId || !code || !arg || !type) {
        console.error(
          "❌ Invalid task received from ORCHESTRATOR. Missing clientId, taskID, code, arg or type."
        );
        ws.send(
          JSON.stringify({
            clientId,
            taskId,
            status: "error",
            result:
              "Invalid task received from ORCHESTRATOR. Missing clientId, taskID, code, arg or type.",
          })
        );
        return;
      }
      console.log(
        `▶️ Worker ${workerId} received task ${taskId} from client ${clientId} with arg ${arg}`
      );
      await executeTask(task, ws, stopWatch); // <-- Execute the task received from the orchestrator
    } catch (err) {
      console.error("❌ Error parsing message from ORCHESTRATOR:", err.message);
    }
  });

  ws.on("error", (err) => {
    console.error("❌ WebSocket ORCHESTRATOR error:", err.message);
  });
  ws.on("close", () =>
    console.log("🔌 WebSocket connection to ORCHESTRATOR closed.")
  );
}
