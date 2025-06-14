import { loadPyodide } from "pyodide";
import axios from "axios";
import WebSocket from "ws"; // Using ws client
import * as Minio from "minio";

const ORCHESTRATOR = "ws://localhost:3000"; // WS endpoint base
const HTTP_ORCH = "http://localhost:3000";

async function getTextFromMinio(fileUrl) {
  const parsed = new URL(fileUrl);
  const minioClient = new Minio.Client({
    endPoint: parsed.hostname,
    port: parseInt(parsed.port),
    useSSL: false,
    accessKey: "minioadmin",
    secretKey: "minioadmin",
  });
  const [, bucket, ...rest] = parsed.pathname.split("/");
  const objectName = rest.join("/");
  const stream = await minioClient.getObject(bucket, objectName);
  return new Promise((res, rej) => {
    let data = "";
    stream.on("data", (c) => (data += c.toString()));
    stream.on("end", () => res(data));
    stream.on("error", (e) => rej(e));
  });
}

let pyodide;

// 1. Inicializa Pyodide y espera a que termine
async function initPy() {
  console.log("⏳ Loading Pyodide...");
  pyodide = await loadPyodide();
  console.log("✅ Pyodide ready");
}
let workerId;
let ws;

// Register with orchestrator and open WS
async function registerAndConnect() {
  try {
    const { data } = await axios.post(`${HTTP_ORCH}/register_worker`, {
      numWorkers: 1,
    });
    workerId = data.worker_id;
    ws = new WebSocket(`${ORCHESTRATOR}?worker_id=${workerId}`);

    ws.on("open", () =>
      console.log(
        `🔌 Connected to ORCHESTRATOR via WebSocket. WORKERID:`,
        workerId
      )
    );

    ws.on("message", async (msg) => {
      const { taskId, code, arg } = JSON.parse(msg.toString());
      console.log(`▶️ Worker ${workerId} received task ${arg}:${taskId}`);

      try {
        const text = await getTextFromMinio(arg);
        const pyScript = `
      ${code}
text = '''${text}'''
result = task(text)
result
      `;
        // console.log(
        //   `📜 Executing task ${arg}:${taskId} with code:\n${pyScript}`
        // );
        //sleep for 10 seconds to simulate a long task
        await new Promise((resolve) => setTimeout(resolve, 10000));
        const result = await pyodide.runPythonAsync(pyScript);
        console.log(`✔️ Completed ${arg}:${taskId}:`, result);
        ws.send(JSON.stringify({ arg, taskId, status: "done", result }));
      } catch (e) {
        console.error(`❌ Error on ${arg}:${taskId}:`, e.message);
        ws.send(
          JSON.stringify({ arg, taskId, status: "error", result: e.message })
        );
      }
    });

    ws.on("close", () =>
      console.log("🔌 WebSocket connection to ORCHESTRATOR closed.")
    );
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

(async () => {
  try {
    await initPy();
    await registerAndConnect();
  } catch (err) {
    console.error("❌ Fatal error during startup:", err);
    process.exit(1);
  }
})();
