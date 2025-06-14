import express from "express";
import { loadPyodide } from "pyodide";
import axios from "axios";
import * as Minio from "minio";

const app = express();
const port = 5000;

async function getTextFromMinio(fileUrl) {
  const parsedUrl = new URL(fileUrl);
  const host = parsedUrl.hostname;
  const port = parseInt(parsedUrl.port, 10);
  const [, bucket, objectName] = parsedUrl.pathname.split("/");

  const minioClient = new Minio.Client({
    endPoint: host,
    port: port,
    useSSL: false,
    accessKey: "minioadmin",
    secretKey: "minioadmin",
  });

  const dataStream = await minioClient.getObject(bucket, objectName);

  return new Promise((resolve, reject) => {
    let fileContent = "";

    dataStream.on("data", (chunk) => {
      fileContent += chunk.toString();
    });

    dataStream.on("end", () => {
      resolve(fileContent);
    });

    dataStream.on("error", (err) => {
      reject(err);
    });
  });
}

// Middleware for parsing JSON requests
app.use(express.json());

// Load Pyodide
let pyodide;
let pyodideReady = false;

async function initPyodide() {
  pyodide = await loadPyodide();
  pyodideReady = true; // Set the flag to true when Pyodide is ready
  console.log("Pyodide loaded");
}
initPyodide();

// Endpoint to execute Python code
app.post("/execute", async (req, res) => {
  if (!pyodideReady) {
    return res
      .status(503)
      .json({ error: "Pyodide is still loading, try again later." });
  }
  const { code, file, task_id } = req.body; // Get the code from the request body

  if (!code || !file || !task_id) {
    return res
      .status(400)
      .json({ error: "Missing code, arguments or task_id." });
  }

  try {
    // Obtain the file URL from the request
    const text = await getTextFromMinio(file);

    const pythonCode = `
    ${code}
text = """${text}"""
result = task(text)  
result
    `;

    // console.log(pythonCode); // For debugging purposes

    // Execute the Python code
    const result = await pyodide.runPythonAsync(pythonCode);

    // Send the result back to the client
    res.json({
      result,
    });
    console.log(
      `🎯 Executed task ${task_id} for arg: ${arg}, result: ${result}`
    );
  } catch (err) {
    res
      .status(500)
      .json({ error: "Error executing Python code", details: err.message });
  }
});

// Endpoint to initialize the server
app.listen(port, () => {
  console.log(`Server is running at http://localhost:${port}`);
});

const numWorkers = 1; // Number of available processors in our worker (1 default)

try {
  const res = await axios.post(`${orchestrator}register_worker`, {
    numWorkers,
  });
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
