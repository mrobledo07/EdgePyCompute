import axios from "axios";
import { io as Client } from "ws"; // Using ws client

const HTTP_ORCH = "http://localhost:3000";
const ORCHESTRATOR = "ws://localhost:3000";
const STORAGE = "http://minio:9000";

// Code
const code = `
def task(text):
    words = text.split()
    return len(words)
`;

// Parameters
const args = [
  `${STORAGE}/test/example1.txt`,
  `${STORAGE}/test/example2.txt`,
  `${STORAGE}/test/example3.txt`,
  `${STORAGE}/test/example4.txt`,
];

const maxTasks = args.length;
let tasksExecuted = 0;
let ws;

async function start() {
  try {
    const res = await axios.post(`${HTTP_ORCH}/register_task`, {
      code,
      args,
    });

    const taskId = res.data.task_id;
    console.log("🚀 Task submitted. ID:", taskId);
    ws = new Client(`${ORCHESTRATOR}/?task_id=${taskId}`);

    ws.on("open", () =>
      console.log("🔌 Connected to ORCHESTRATOR via WebSocket. ID:", taskId)
    );

    ws.on("message", (event) => {
      tasksExecuted++;
      if (tasksExecuted >= maxTasks) {
        console.log("✅ All tasks executed successfully.");
        ws.close();
      } else {
        const data = JSON.parse(event.data);
        const { arg, status, result } = data;
        console.log(
          `📦 Task ${arg} executed. Status: ${status}. Result: ${result}`
        );
      }
    });

    ws.on("close", () => {
      console.log("🔌 WebSocket connection to ORCHESTRATOR closed.");
    });

    ws.on("error", (err) => {
      console.error("❌ WebSocket ORCHESTRATOR error:", err.message);
    });
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
}

start();
