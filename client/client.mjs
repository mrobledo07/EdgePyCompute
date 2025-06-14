import axios from "axios";
import WebSocket from "ws";

const orchestrator = "http://localhost:3000/";
const wsUrl = "ws://localhost:3000/ws";
const storage = "http://minio:9000/";

const code = `
def task(text):
    words = text.split()
    return len(words)
`;

// Parameters
const args = [`${storage}test/example.txt`, `${storage}test/example2.txt`];
const maxTasks = args.length;
let tasksExecuted = 0;

async function start() {
  try {
    const res = await axios.post(`${orchestrator}register_task`, {
      code,
      args,
    });

    const taskId = res.data.task_id;
    console.log("🚀 Task submitted. ID:", taskId);
    const ws = new WebSocket(`${wsUrl}?task_id=${taskId}`);

    ws.onopen = () => {
      console.log("🔌 Connected to orchestrator via WebSocket");
    };

    ws.onmessage = (event) => {
      tasksExecuted++;
      if (tasksExecuted >= maxTasks) {
        console.log("✅ All tasks executed successfully.");
        ws.close();
      } else {
        const data = JSON.parse(event.data);
        const { arg, result } = data;
        console.log(`📦 Result for ${arg}:`, result);
      }
    };

    ws.onclose = () => {
      console.log("🔌 WebSocket connection closed");
    };

    ws.onerror = (err) => {
      console.error("❌ WebSocket error:", err.message);
    };
  } catch (err) {
    if (err.response) {
      console.error(
        "❌ Error response from orchestrator server:",
        err.response.data || err.message
      );
    } else if (err.request) {
      console.error(
        "❌ No response received from the orchestrator server. The server may be down."
      );
    } else {
      console.error("❌ Error:", err.message);
    }
  }
}
