import { initPyodide } from "./pyodideRuntime.mjs";
import { registerWorker } from "./worker.mjs";

(async () => {
  try {
    await initPyodide();
    await registerWorker();
  } catch (e) {
    console.error("❌ Fatal error:", e.message);
    process.exit(1);
  }
})();
