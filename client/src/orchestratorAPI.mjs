// orchestratorAPI.js
import axios from "axios";
import WebSocket from "ws";
import readline from "readline/promises";

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

// export async function connectClient(clientId, httpUrl) {
//   try {
//     const res = await axios.post(`${httpUrl}/client_connect`, {
//       client_id: clientId,
//     });
//     console.log("🔌 Connected to orchestrator. CLIENT ID:", res.data.client_id);
//     return {
//       results: res.data.results,
//       pendingTasks: res.data.pendingTasks || 0,
//     };
//   } catch (err) {
//     if (err.response) {
//       console.error("❌ Response error:", err.response.data || err.message);
//     } else if (err.request) {
//       console.error("❌ No response from orchestrator.");
//     } else {
//       console.error("❌ Error:", err.message);
//     }
//   }
// }

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

function printMetadata(metadata) {
  console.log("📊 Execution Metadata:");

  Object.entries(metadata).forEach(([taskId, times], index) => {
    const [startTime, readTime, cpuTime, writeTime, endTime] = times;

    console.log(`- ${index + 1}`);
    console.log(`  • Task ID    : ${taskId}`);
    console.log(`  • Start Time : ${startTime.toFixed(3)}`);
    console.log(`  • Read time  : ${readTime.toFixed(4)}s`);
    console.log(`  • CPU time   : ${cpuTime.toFixed(4)}s`);
    console.log(`  • Write time : ${writeTime.toFixed(4)}s`);
    console.log(`  • End Time   : ${endTime.toFixed(3)}`);
  });

  console.log("✅ All tasks executed.");
}

export function connectToWebSocket(wsUrl, clientId, maxTasks, stopwatches) {
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
      const { message_type, taskId, status, result, metadata } = JSON.parse(
        data.toString()
      );

      const metadataParsed = metadata ? JSON.parse(metadata) : {};

      switch (message_type) {
        case "task_result":
          stopwatches[tasksExecuted].stop();
          const executionTimeClient = parseFloat(
            stopwatches[tasksExecuted].getDuration().toFixed(4)
          );
          tasksExecuted++;
          //const roundTo4 = (num) => Math.round(num * 10000) / 10000;

          console.log(
            `🕒 Task ${taskId} completed. Execution time CLIENT: ${executionTimeClient} seconds.`
          );
          console.log(
            `📦 Task ${taskId}. Status: ${status}. Result: ${result}.`
          );

          printMetadata(metadataParsed);
          if (tasksExecuted >= maxTasks) {
            console.log("✅ All tasks executed.");
            ws.close();
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
