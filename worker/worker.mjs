import { loadPyodide } from "pyodide";
import axios from "axios";
import WebSocket from "ws"; // Using ws client
import * as Minio from "minio";

const ORCHESTRATOR = "ws://localhost:3000"; // WS endpoint base
const HTTP_ORCH = "http://localhost:3000";
const STORAGE = "http://localhost:9002"; // Minio HTTP endpoint orchestrator

let minioClient;
const createMinioClient = (fileURL) => {
  const parsed = new URL(fileURL);
  minioClient = new Minio.Client({
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
  createMinioClient(parsed);
  const { bucket, objectName } = obtainBucketAndObjectName(fileUrl);
  let stream;
  if (offset == -1) {
    stream = await minioClient.getObject(bucket, objectName);
  } else {
    // If offset is provided, get a partial object

    const stat = await minioClient.statObject(bucket, objectName);
    const totalSize = stat.size;
    const chunkSize = Math.floor(totalSize / numMappers);
    const start = offset * chunkSize;
    let end = (offset + 1) * chunkSize - 1;

    // Assign the last chunk to the last mapper
    if (offset === numMappers - 1) {
      end = totalSize - 1;
    }
    stream = await minioClient.getPartialObject(bucket, objectName, start, end);
  }
  return new Promise((res, rej) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("end", () => res(Buffer.concat(chunks)));
    stream.on("error", (e) => rej(e));
  });
}

let pyodide;

// 1. Inicializa Pyodide y espera a que termine
async function initPy() {
  console.log("⏳ Loading Pyodide...");
  pyodide = await loadPyodide();
  await pyodide.loadPackage("pandas");
  await pyodide.runPythonAsync(`
# pyedgecompute.py
import pickle, base64, json
import pandas as pd
import numpy as np


# CODE MAPREDUCE
def serialize_partition(result):
    raw = pickle.dumps(result)
    return base64.b64encode(raw).decode('utf-8')

def deserialize_partitions(b64_list):
    parts = []
    for b64 in b64_list:
        raw_bytes = base64.b64decode(b64)
        part = pickle.loads(raw_bytes)
        parts.append(part)
    return parts

def deserialize_input_string(bytes_string):
    string_data = bytes_string.decode('utf-8')
    return string_data

# CODE TERASORT
def deserialize_input_terasort(data):
    lines = data.split(b'\\n')
    result = {}
    for line in lines:
        if len(line) >= 98:
            key = line[:10].decode('utf-8', errors='ignore')
            value = line[12:98].decode('utf-8', errors='ignore')
            result[key] = value
    df = pd.DataFrame(list(result.items()), columns=["0", "1"])
    result = df
    return result

MIN_CHAR_ASCII = 32  # ' '
MAX_CHAR_ASCII = 126 # '~'
range_per_char = MAX_CHAR_ASCII - MIN_CHAR_ASCII
base = range_per_char + 1

def get_partition(line, num_partitions):

    numerical_value = 0
    max_numerical_value = 0
    for i in range(8):
        if i < len(line):
            normalized_char_val = ord(line[i]) - MIN_CHAR_ASCII
            numerical_value = numerical_value * base + normalized_char_val
        else:
            normalized_char_val = 0
            numerical_value = numerical_value * base + normalized_char_val

    for i in range(8):
        max_numerical_value = max_numerical_value * base + range_per_char

    if max_numerical_value == 0:
        return 0

    normalized_value = numerical_value / max_numerical_value
    partition = int(normalized_value * num_partitions)

    if partition >= num_partitions:
        partition = num_partitions - 1

    return partition

def partition_data(data, num_partitions):
    partitions = {i: None for i in range(num_partitions)}
    partition_indices = np.empty(len(data), dtype=np.int32)

    for idx, key in enumerate(data["0"]):
        partition_indices[idx] = get_partition(key, num_partitions)

    for i in range(num_partitions):
        indices = np.where(partition_indices == i)[0]
        if indices.size > 0:
            partitions[i] = data.iloc[indices].reset_index(drop=True)
        else:
            partitions[i] = pd.DataFrame(columns=data.columns)

    return partitions

def concat_partitions(partition_list):

    if not partition_list:
        return pd.DataFrame(columns=["0", "1"])

    return pd.concat(partition_list, ignore_index=True)

def sort_dataframe(df):

    return df.sort_values(by=["0"]).reset_index(drop=True)

# Inject module
import types
pyedgecompute = types.ModuleType("pyedgecompute")
pyedgecompute.serialize_partition = serialize_partition
pyedgecompute.deserialize_partitions = deserialize_partitions
pyedgecompute.deserialize_input_string = deserialize_input_string
pyedgecompute.deserialize_input_terasort = deserialize_input_terasort
pyedgecompute.partition_data = partition_data
pyedgecompute.concat_partitions = concat_partitions
pyedgecompute.sort_dataframe = sort_dataframe


import sys
sys.modules["pyedgecompute"] = pyedgecompute
`);
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
  console.log(`🔍 Getting TASK ARG ${task.arg}`);
  const text = await getTextFromMinio(task.arg, offset, numMappers);
  return text;
}

async function getSerializedMappersResults(results) {
  if (!Array.isArray(results) || results.length === 0)
    throw new Error(
      "Invalid task received. Missing array of results for REDUCER."
    );

  const b64List = [];
  for (const result of results) {
    let partialResult = await getTextFromMinio(result);
    console.log("🔍 First partial:", partialResult);
    b64List.push(partialResult.toString("utf-8"));
  }
  //console.log("🔍 Mappers results aggregated:", resultJSON);
  console.log("🔍 Returning serialized results for REDUCER:", b64List);
  // Return the
  return b64List;
}

/**
 * Returns true if the object is a PyProxy object.
 * This is used to check if the object is a Pyodide proxy object.
 * @param {Object} obj - The object to check.
 * @returns {boolean} - True if the object is a PyProxy object, false otherwise
 */
function isPyProxy(obj) {
  return (
    obj != null &&
    typeof obj.toJs === "function" &&
    typeof obj.destroy === "function"
  );
}

async function setSerializedMapperResult(task, result) {
  const basePath = `${STORAGE}/${task.taskId}/${workerId}`;
  createMinioClient(basePath);
  const { bucket } = obtainBucketAndObjectName(basePath);

  try {
    await minioClient.makeBucket(bucket, "us-east-1");
  } catch (e) {
    if (e.code !== "BucketAlreadyOwnedByYou") {
      console.error(`❌ Error creating bucket ${bucket}:`, e.message);
      throw e;
    }
    console.log(`ℹ️ Bucket ${bucket} already exists, skipping creation.`);
  }

  if (Array.isArray(result)) {
    console.log("WE ARE IN REDUCER TERASORT ARRAY");
    // TERASORT REDUCER: result is an array
    const urls = [];
    for (let i = 0; i < result.length; i++) {
      const reducerResult = result[i];
      const objectName = `${task.numWorker}_${i}.txt`;

      console.log(
        `📦 Storing reducer result in Minio: ${basePath}/${objectName}`
      );
      try {
        await minioClient.putObject(
          bucket,
          `${workerId}/${objectName}`,
          Buffer.from(reducerResult),
          reducerResult.length,
          "application/json"
        );
        urls.push(`${basePath}/${objectName}`);
      } catch (e) {
        console.error(`❌ Error storing reducer result [${i}]:`, e.message);
        throw e;
      }
    }
    return urls; // Optional: returns all URLs
  } else {
    console.log("WE ARE WHERE WE SHOULD NOT BE");
    // NORMAL CASE
    const objectName = `${task.numWorker}.txt`;

    console.log(`📦 Storing result in Minio: ${basePath}/${objectName}`);
    try {
      await minioClient.putObject(
        bucket,
        `${workerId}/${objectName}`,
        Buffer.from(result),
        result.length,
        "application/json"
      );
      return `${basePath}/${objectName}`;
    } catch (e) {
      console.error(`❌ Error storing result in Minio:`, e.message);
      throw e;
    }
  }
}

async function executeCodeAndSendResult(task) {
  try {
    let bytes;
    if (task.type === "mapwordcount" || task.type === "mapterasort") {
      console.log(
        `🔍 Getting partial object for MAPPER task ${task.arg}:${task.taskId}`
      );
      //
      bytes = await getPartialObjectMinio(task);
    } else if (
      task.type === "reducewordcount" ||
      task.type === "reduceterasort"
    ) {
      console.log(
        `🔍 Getting serialized results for REDUCER task ${task.arg}:${task.taskId}`
      );
      bytes = await getSerializedMappersResults(task.arg);
    } else {
      console.log(
        `🔍 Getting full object for task ${task.arg}:${task.taskId} (not
        map or reduce)`
      );
      bytes = await getTextFromMinio(task.arg);
    }

    let rawBytesLine;
    if (task.type === "reduceterasort" || task.type === "reducewordcount") {
      // Reducer: arg es un JSON‐string con ["b64part1","b64part2",...]
      // Pasamos esa cadena TEXTUAL directamente a Python
      rawBytesLine = `raw_bytes = ${JSON.stringify(bytes)}`;
    } else {
      // Map: bytes es un Buffer → lo pasamos como Base64 y DECODIFICAMOS en Python
      const b64 = bytes.toString("base64");
      rawBytesLine = `raw_bytes = base64.b64decode("${b64}")`;
    }

    const pyScript = `
${task.code}
${rawBytesLine}
try:
    result = task(raw_bytes)
except Exception as e:
    result = str(e)
result
      `;
    console.log(
      `📜 Executing task ${task.arg}:${task.taskId} with code:\n${pyScript}`
    );
    //sleep for 3 seconds to simulate a long task
    // await new Promise((resolve) => setTimeout(resolve, 3000));
    let result = await pyodide.runPythonAsync(pyScript);
    console.log(`✔️ Completed ${task.arg}:${task.taskId}:`, result);

    if (task.type === "mapterasort") {
      result = JSON.parse(result);
    }

    console.log("🔍 typeof result:", typeof result);
    console.log("🔍 instanceof Array:", result instanceof Array);
    console.log("🔍 isPyProxy:", isPyProxy(result));

    if (task.type === "mapwordcount" || task.type === "mapterasort") {
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
    } else if (task.type === "reduceterasort") {
      /* Code if we want to change output of reduce terasort (now is pickle and base64)*/
      // result = Buffer.from(result, "base64").toString("utf-8"); <-- Only if result is a CSV
      // result = JSON.parse(result);
      ws.send(
        JSON.stringify({
          arg: task.arg,
          taskId: task.taskId,
          status: "done",
          result,
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
