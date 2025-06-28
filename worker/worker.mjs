import { loadPyodide } from "pyodide";
import axios from "axios";
import WebSocket from "ws"; // Using ws client
import * as Minio from "minio";

const ORCHESTRATOR = "ws://localhost:3000"; // WS endpoint base
const HTTP_ORCH = "http://localhost:3000";

let minioClient;
let host;
let port;

const createMinioClient = (fileURL) => {
  const parsed = new URL(fileURL);
  minioClient = new Minio.Client({
    endPoint: parsed.hostname,
    port: parseInt(parsed.port),
    useSSL: false,
    accessKey: "minioadmin",
    secretKey: "minioadmin",
  });
  host = parsed.hostname;
  port = parsed.port;
};

const obtainBucketAndObjectName = (fileUrl) => {
  const parsed = new URL(fileUrl);
  const [, bucket, ...rest] = parsed.pathname.split("/");
  const objectName = rest.join("/");
  return { bucket, objectName };
};

async function getTextFromMinio(fileUrl, offset = -1, numMappers = -1) {
  const parsed = new URL(fileUrl);
  createMinioClient(parsed);
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
  console.log(
    `🔍 Getting partial object from Minio: offset=${offset}, numMappers=${numMappers}`
  );
  if (offset === undefined || numMappers === undefined) {
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
    let partialResult = await getTextFromMinio(result);
    // Make sure the result is a string
    if (typeof partialResult !== "string") {
      partialResult = partialResult.toString("utf-8");
    }

    // Double-check that the partial result is a valid JSON string
    let parsed = JSON.parse(partialResult);
    if (typeof parsed === "string") {
      parsed = JSON.parse(parsed);
    }

    if (typeof parsed !== "object" || parsed === null) {
      throw new Error("Parsed JSON is not a valid object");
    }

    console.log("✅ Partial result JSON:", parsed);

    for (const key in parsed) {
      if (!resultJSON[key]) {
        resultJSON[key] = [];
      }
      resultJSON[key].push(parsed[key]);
    }
  }
  //console.log("🔍 Mappers results aggregated:", resultJSON);
  console.log(
    "🔍 Returning serialized results for REDUCER:",
    JSON.stringify(resultJSON)
  );
  // Return the
  return JSON.stringify(resultJSON);
}

async function setSerializedMapperResult(task, result) {
  const resultURL = `http://${host}:${port}/${task.taskId}/${workerId}/${task.numWorker}.txt`;
  const resultJSON = JSON.stringify(result);
  const { bucket, objectName } = obtainBucketAndObjectName(resultURL);
  // minioClient is created already in getTextFromMinio
  try {
    await minioClient.makeBucket(bucket, "us-east-1");
  } catch (e) {
    if (e.code !== "BucketAlreadyOwnedByYou") {
      console.error(`❌ Error creating bucket ${bucket}:`, e.message);
      throw e;
    }
    console.log(`ℹ️ Bucket ${bucket} already exists, skipping creation.`);
  }
  console.log(`📦 Storing result in Minio: ${resultURL}`);
  // Store the result in Minio
  try {
    await minioClient.putObject(
      bucket,
      objectName,
      Buffer.from(resultJSON),
      resultJSON.length,
      "application/json"
    );
  } catch (e) {
    console.error(`❌ Error storing result in Minio:`, e.message);
    throw e;
  }
  return resultURL;
}

async function executeCodeAndSendResult(task) {
  try {
    let text;
    if (task.type == "map") {
      console.log(
        `🔍 Getting partial object for MAPPER task ${task.arg}:${task.taskId}`
      );
      //
      text = await getPartialObjectMinio(task);
    } else if (task.type == "reduce") {
      console.log(
        `🔍 Getting serialized results for REDUCER task ${task.arg}:${task.taskId}`
      );
      text = await getSerializedMappersResults(task.arg);
    } else {
      console.log(
        `🔍 Getting full object for task ${task.arg}:${task.taskId} (not
        map or reduce)`
      );
      text = await getTextFromMinio(task.arg);
    }

    let textLiteral;
    if (task.type === "reduce") {
      textLiteral = text;
    } else {
      textLiteral = `'''${text}'''`;
    }

    const pyScript = `
      ${task.code}
text = ${textLiteral}
try:
    result = task(text)
except Exception as e:
    result = str(e)
result
      `;
    console.log(
      `📜 Executing task ${task.arg}:${task.taskId} with code:\n${pyScript}`
    );
    //sleep for 3 seconds to simulate a long task
    // await new Promise((resolve) => setTimeout(resolve, 3000));
    const result = await pyodide.runPythonAsync(pyScript);
    console.log(`✔️ Completed ${task.arg}:${task.taskId}:`, result);

    if (task.type == "map") {
      const resultURL = await setSerializedMapperResult(task, result);
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
