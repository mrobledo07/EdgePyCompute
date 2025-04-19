import express from "express";
import { loadPyodide } from "pyodide";

const app = express();
const port = 3000;

app.use(express.json());

let pyodide;

const init = async () => {
  pyodide = await loadPyodide();
  console.log("✅ Pyodide loaded");
};

app.post("/run", async (req, res) => {
  const { code } = req.body;

  if (!code) {
    return res.status(400).json({ error: "No Python code provided" });
  }

  try {
    const result = await pyodide.runPythonAsync(code);
    res.json({ result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(port, async () => {
  await init();
  console.log(`🚀 Server listening at http://localhost:${port}`);
});
