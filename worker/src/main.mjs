import { initPyodide } from "./pyodideRuntime.mjs";
import { registerWorker } from "./orchestratorAPI.mjs";
// import { HTTP_ORCH, WS_ORCH } from "./configMinio.mjs";
// import { HTTP_ORCH_AWS, WS_ORCH_AWS } from "./configAWS.mjs";

//let HTTP_ORCH_USED, WS_ORCH_USED;

// let HTTP_ORCH = null;
// let WS_ORCH = null;
// let STORAGE = null;

import { obtainArgs } from "./utils.mjs";

export const CONFIG = {
  STORAGE: null,
  HTTP_ORCH: null,
  WS_ORCH: null,
};

(async () => {
  try {
    obtainArgs(CONFIG);
    await initPyodide();
    await registerWorker();
  } catch (e) {
    console.error("❌ Fatal error:", e.message);
    process.exit(1);
  }
})();
