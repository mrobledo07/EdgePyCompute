import express from "express";
import { loadPyodide } from "pyodide";

const app = express();
const port = 3000;

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

const base = 7;
// Endpoint to execute Python code
app.post("/run", async (req, res) => {
  if (!pyodideReady) {
    return res
      .status(503)
      .json({ error: "Pyodide is still loading, try again later." });
  }
  const { code } = req.body; // Get the code from the request body

  if (!code) {
    return res.status(400).json({ error: "Missing code !" });
  }

  try {
    const pythonCode = `
    ${code}
result = pow(${base}, 2)  
result
    `;

    console.log(pythonCode);

    // Execute the Python code
    const result = await pyodide.runPythonAsync(pythonCode);

    // Send the result back to the client
    res.json({
      result: result,
      argument: base,
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
