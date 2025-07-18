import fs from "fs/promises";
import axios from "axios";
import WebSocket from "ws"; // Using ws client
import readline from "readline/promises";

const HTTP_ORCH = "http://localhost:3000";
const WS_ORCH = "ws://localhost:3000";

const configPath = process.argv[2]; // Load from CLI argument

let tasksExecuted = 0;
let ws = null;
let taskId = -1;
let maxTasks = -1;
let task = null;

async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

async function loadConfig(filePath) {
  if (!(await fileExists(filePath))) {
    throw new Error(`Config file "${filePath}" does not exist.`);
  }

  const raw = await fs.readFile(filePath, "utf-8");
  const config = JSON.parse(raw);

  if (!config.type || !config.code || !config.args) {
    throw new Error(
      `Config must include 'type', 'code' (array of file paths), and 'args'.`
    );
  }

  if (!Array.isArray(config.code) || config.code.length === 0) {
    throw new Error(`'code' must be a non-empty array of file paths.`);
  }

  if (!Array.isArray(config.args) || config.args.length === 0) {
    throw new Error(`'args' must be a non-empty array of arguments.`);
  }

  for (const file of config.code) {
    if (!(await fileExists(file))) {
      throw new Error(`Code file "${file}" does not exist.`);
    }
  }

  return config;
}

async function loadCode(files) {
  const codeParts = await Promise.all(
    files.map((file) => fs.readFile(file, "utf-8"))
  );
  return codeParts;
}

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function sendTaskWithRetry(task, HTTP_ORCH) {
  while (true) {
    try {
      const res = await axios.post(`${HTTP_ORCH}/register_task`, task);
      taskId = res.data.task_id;
      console.log("🚀 Task submitted. TASK ID:", taskId);
      break; // Exit loop on success
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

      try {
        await rl.question("\n🔁 Press Enter to retry, or Ctrl+C to exit...\n");
      } catch (e) {
        // User pressed Ctrl+C
        console.log("\n👋 Exiting...");
        process.exit(1);
      }
    }
  }
  rl.close();
}

async function start() {
  if (!configPath) {
    console.error("❌ Please provide the path to a config file.");
    process.exit(1);
  }

  try {
    const config = await loadConfig(configPath);
    const codeParts = await loadCode(config.code);
    maxTasks = config.args.length;
    task = {
      type: config.type,
      args: config.args,
      code: codeParts,
    };
  } catch (err) {
    console.error("❌ Error loading configuration:", err.message);
    process.exit(1);
  }

  await sendTaskWithRetry(task, HTTP_ORCH);

  ws = new WebSocket(`${WS_ORCH}?task_id=${taskId}`);

  ws.on("open", () =>
    console.log("🔌 Connected to ORCHESTRATOR via WebSocket. TASKID:", taskId)
  );

  ws.on("message", (data) => {
    try {
      const parsed = JSON.parse(data.toString());

      const { message_type, arg, status, result } = parsed;

      switch (message_type) {
        case "task_result":
          tasksExecuted++;
          console.log(
            `📦 Task ${taskId}:[${arg}] executed. Status: ${status}. Result: ${result}`
          );
          if (tasksExecuted >= maxTasks) {
            console.log("✅ All tasks executed.");
          }
          break;

        case "info":
          console.log(`ℹ️ Info: ${result}`);
          break;

        case "error":
          console.error(`❌ Error reported by orchestrator: ${result}`);
          break;

        default:
          console.warn(`⚠️ Unknown message_type: "${message_type}"`);
      }
    } catch (err) {
      console.error("❌ Error parsing message from ORCHESTRATOR:", err.message);
    }
  });

  ws.on("close", () => {
    console.log("🔌 WebSocket connection to ORCHESTRATOR closed.");
  });

  ws.on("error", (err) => {
    console.error("❌ WebSocket ORCHESTRATOR error:", err.message);
  });
}

start();
