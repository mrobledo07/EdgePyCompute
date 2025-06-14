import { loadPyodide } from "pyodide";
import axios from "axios";
import { io as Client } from "ws"; // Using ws client
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

// Load Pyodide\let pyodide, pyReady=false;
async function initPy() {
  pyodide = await loadPyodide();
  pyReady = true;
  console.log("Pyodide ready");
}
initPy();

let workerId;
let ws;

// Register with orchestrator and open WS
async function registerAndConnect() {
  const { data } = await axios.post(`${HTTP_ORCH}/register_worker`, {
    numWorkers: 1,
  });
  workerId = data.worker_id;
  ws = new Client(`${ORCHESTRATOR}/?worker_id=${workerId}`);

  ws.on("open", () =>
    console.log(`🔌 Connected to ORCHESTRATOR via WebSocket. ID:`, workerId)
  );

  ws.on("message", async (msg) => {
    const { taskId, code, arg } = JSON.parse(msg.toString());
    console.log(`▶️ Received task ${arg}:${taskId}`);

    if (!pyReady) {
      console.error(`Pyodide not ready for task ${arg}:${taskId}`);
      ws.send(
        JSON.stringify({
          arg,
          taskId,
          status: "error",
          error: "Pyodide not loaded, try again later.",
        })
      );
      return;
    }

    try {
      const text = await getTextFromMinio(arg);
      const pyScript = `
${code}
text = '''${text}'''
result = task(text)
result
      `;
      const result = await pyodide.runPythonAsync(pyScript);
      console.log(`✔️ Completed ${arg}:${taskId}:`, result);
      ws.send(JSON.stringify({ arg, taskId, status: "done", result }));
    } catch (e) {
      console.error(`❌ Error on ${arg}:${taskId}:`, e);
      ws.send(
        JSON.stringify({ arg, taskId, status: "error", result: e.message })
      );
    }
  });

  ws.on("close", () => console.log("WS closed"));
}

registerAndConnect();

app.post("/health", (req, res) => res.sendStatus(200));

app.listen(port, () => console.log(`Worker HTTP listening on ${port}`));
