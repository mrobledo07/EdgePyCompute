import axios from "axios";
import WebSocket from "ws"; // Using ws client

const HTTP_ORCH = "http://orchestrator:3000";
const ORCHESTRATOR = "ws://orchestrator:3000";
const STORAGE = "http://localhost:9000";

// Code
const codeMap = `
import pickle
import base64
from collections import Counter
def task(text):
    words = text.split()
    counter = Counter(words)
    # Serialize to bytes using pickle
    serialized = pickle.dumps(counter)
    # Encode as base64 string for compatibility with JS
    encoded = base64.b64encode(serialized).decode('utf-8')
    return encoded
`;

const codeReduce = `
import json
import pickle
import base64
from collections import Counter
def task(text):
    if isinstance(text, str):
        b64_list = json.loads(text)
    else:
        b64_list = text
    final_counter = Counter()
    for b64 in b64_list:
        # 1) convert Base64 → bytes
        raw = base64.b64decode(b64)
        # 2) unpickle → Counter parcial
        part = pickle.loads(raw)
        # 3) reduce → Counter final
        final_counter.update(part)
    # 4) (opcional) serialize again to Pickle+Base64
    # serialized = pickle.dumps(final_counter)
    return json.dumps(final_counter)
`;

const code = [codeMap, codeReduce];

// Parameters
const args = [
  `${STORAGE}/test/example1.txt`,
  //`${STORAGE}/test/example2.txt`,
  //`${STORAGE}/test/example3.txt`,
  //`${STORAGE}/test/example4.txt`,
];

const maxTasks = args.length;
let tasksExecuted = 0;
let ws;

async function start() {
  try {
    const res = await axios.post(`${HTTP_ORCH}/register_task`, {
      code,
      args,
      type: "mapreduce",
    });

    const taskId = res.data.task_id;
    console.log("🚀 Task submitted. ID:", taskId);
    ws = new WebSocket(`${ORCHESTRATOR}?task_id=${taskId}`);

    ws.on("open", () =>
      console.log("🔌 Connected to ORCHESTRATOR via WebSocket. TASKID:", taskId)
    );

    ws.on("message", (data) => {
      tasksExecuted++;

      try {
        const { arg, status, result } = JSON.parse(data.toString());
        console.log(
          `📦 Task ${taskId}:[${arg}] executed. Status: ${status}. Result: ${result}`
        );

        if (tasksExecuted >= maxTasks) {
          console.log("✅ All tasks executed.");
        }
      } catch (err) {
        console.error(
          "❌ Error parsing message from ORCHESTRATOR:",
          err.message
        );
      }
    });

    ws.on("close", () => {
      console.log(
        "🔌 WebSocket connection to ORCHESTRATOR closed. TASKID:",
        taskId
      );
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
