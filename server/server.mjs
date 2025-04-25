import express from "express";
import { loadPyodide } from "pyodide";
// import axios from "axios";
import * as Minio from "minio";

const app = express();
const port = 3000;

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
app.post("/run", async (req, res) => {
  if (!pyodideReady) {
    return res
      .status(503)
      .json({ error: "Pyodide is still loading, try again later." });
  }
  const { code, file } = req.body; // Get the code from the request body

  if (!code || file === undefined) {
    return res.status(400).json({ error: "Missing code or arguments." });
  }

  try {
    // Obtain the file URL from the request
    const text = await getTextFromMinio(file);

    const pythonCode = `
    ${code}
text = """${text}"""
result = word_count(text)  
result
    `;

    // console.log(pythonCode); // For debugging purposes

    // Execute the Python code
    const result = await pyodide.runPythonAsync(pythonCode);

    // Send the result back to the client
    res.json({
      result,
      file,
    });
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
