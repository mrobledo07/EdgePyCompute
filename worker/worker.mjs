import { loadPyodide } from "pyodide";
import axios from "axios";
import WebSocket from "ws"; // Using ws client
import * as Minio from "minio";

const ORCHESTRATOR = "ws://localhost:3000"; // WS endpoint base
const HTTP_ORCH = "http://localhost:3000";

let minioClient;

const createMinioClient = (fileURL) => {
  const parsed = new URL(fileURL);
  return new Minio.Client({
    endPoint: parsed.hostname,
    port: parseInt(parsed.port),
    useSSL: false,
    accessKey: "minioadmin",
    secretKey: "minioadmin",
  });
};

const obtainBucketAndObjectName = (fileUrl) => {
  const parsed = new URL(fileUrl);
  const [, bucket, ...rest] = parsed.pathname.split("/");
  const objectName = rest.join("/");
  return { bucket, objectName };
};

async function getTextFromMinio(fileUrl, offset = -1, numMappers = -1) {
  const parsed = new URL(fileUrl);
  minioClient = createMinioClient(parsed);
  const { bucket, objectName } = obtainBucketAndObjectName(fileUrl);
  let stream;
  if (offset == -1) {
    stream = await minioClient.getObject(bucket, objectName);
  } else {
    // If offset is provided, get a partial object

    const stat = await minioClient.statObject(bucket, objectName);
    const length = stat.size / numMappers;
    stream = await minioClient.getPartialObject(
      bucket,
      objectName,
      offset,
      length
    );
  }
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

async function getPartialObjectMinio(task) {
  const offset = task.numWorker;
  const numMappers = task.numMappers;
  if (!offset || !numMappers) {
    console.error(
      "❌ Invalid task received. Missing offset or num of mappers for MAPPER partial object."
    );
    throw new Error(
      "Invalid task received. Missing offset or num of mappers for MAPPER partial object."
    );
  }

  const text = await getTextFromMinio(task.arg, offset, numMappers);
  return text;
}

async function getSerializedMappersResults(results) {
  if (!Array.isArray(results) || results.length === 0)
    throw new Error(
      "Invalid task received. Missing array of results for REDUCER."
    );

  const resultJSON = {};
  for (const result of results) {
    const partialResult = await getTextFromMinio(result);
    const deserializedJSONResult = JSON.parse(partialResult);
    for (const [, count] of deserializedJSONResult) {
      if (!resultJSON.word) resultJSON.word = [count];
      else resultJSON.word.push(count);
    }
  }
  return JSON.stringify(resultJSON);
}

async function setSerializedMapperResult(result) {
  const resultURL = "${task.taskId}/${workerId}/${task.numWorker}.txt";
  const resultJSON = JSON.stringify(result);
  const { bucket, objectName } = obtainBucketAndObjectName(resultURL);
  // minioClient is created already in getTextFromMinio
  await minioClient.putObject(
    bucket,
    objectName,
    Buffer.from(resultJSON),
    resultJSON.length,
    "application/json"
  );
  return resultURL;
}

async function executeCodeAndSendResult(task) {
  try {
    let text;
    if (task.type == "map") {
      text = await getPartialObjectMinio(task);
    } else if (task.type == "reduce") {
      text = await getSerializedMappersResults(task.arg);
    } else {
      text = await getTextFromMinio(task.arg);
    }

    const pyScript = `
      ${task.code}
text = '''${text}'''
result = task(text)
result
      `;
    // console.log(
    //   `📜 Executing task ${arg}:${taskId} with code:\n${pyScript}`
    // );
    //sleep for 3 seconds to simulate a long task
    await new Promise((resolve) => setTimeout(resolve, 3000));
    const result = await pyodide.runPythonAsync(pyScript);
    console.log(`✔️ Completed ${task.arg}:${task.taskId}:`, result);

    if (task.type == "map") {
      const resultURL = setSerializedMapperResult(result);
      // Create resultUrl
      ws.send(
        JSON.stringify({
          arg: task.arg,
          taskId: task.taskId,
          status: "done",
          result: resultURL,
        })
      );
    } else {
      ws.send(
        JSON.stringify({
          arg: task.arg,
          taskId: task.taskId,
          status: "done",
          result,
        })
      );
    }
  } catch (e) {
    console.error(`❌ Error on ${task.arg}:${task.taskId}:`, e.message);
    ws.send(
      JSON.stringify({
        arg: task.arg,
        taskId: task.taskId,
        status: "error",
        result: e.message,
      })
    );
  }
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

      executeCodeAndSendResult(task);
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
