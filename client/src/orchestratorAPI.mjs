// orchestratorAPI.js
import axios from "axios";
import WebSocket from "ws";
import readline from "readline/promises";

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

export async function sendTaskWithRetry(task, httpUrl) {
  while (true) {
    try {
      const res = await axios.post(`${httpUrl}/register_task`, task);
      console.log("🚀 Tasks submitted. CLIENT ID:", res.data.client_id);
      return res.data.client_id;
    } catch (err) {
      if (err.response) {
        console.error("❌ Response error:", err.response.data || err.message);
      } else if (err.request) {
        console.error("❌ No response from orchestrator.");
      } else {
        console.error("❌ Error:", err.message);
      }

      try {
        await rl.question("\n🔁 Press Enter to retry, or Ctrl+C to exit...\n");
      } catch {
        console.log("\n👋 Exiting...");
        process.exit(1);
      }
    }
  }
}

export function connectToWebSocket(wsUrl, clientId, maxTasks) {
  const ws = new WebSocket(`${wsUrl}?client_id=${clientId}`);
  let tasksExecuted = 0;

  ws.on("open", () => {
    console.log(
      "🔌 Connected to orchestrator via WebSocket. CLIENT ID:",
      clientId
    );
  });

  ws.on("message", (data) => {
    try {
      const { message_type, arg, taskId, status, result } = JSON.parse(
        data.toString()
      );

      switch (message_type) {
        case "task_result":
          tasksExecuted++;
          console.log(
            `📦 Task ${taskId}. Status: ${status}. Result: ${result}`
          );
          if (tasksExecuted >= maxTasks) {
            console.log("✅ All tasks executed.");
          }
          break;

        case "info":
          console.log(`ℹ️ Info: ${result}`);
          break;

        case "error":
          console.error(`❌ Orchestrator error: ${result}`);
          break;

        default:
          console.warn(`⚠️ Unknown message_type: ${message_type}`);
      }
    } catch (err) {
      console.error("❌ WebSocket message error:", err.message);
    }
  });

  ws.on("close", () => {
    console.log("🔌 WebSocket connection closed.");
    process.exit(0);
  });
  ws.on("error", (err) => {
    console.error("❌ WebSocket error:", err.message);
    process.exit(1);
  });
}
