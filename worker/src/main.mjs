import { initPyodide } from "./pyodideRuntime.mjs";
import { registerWorker } from "./orchestratorAPI.mjs";
import { HTTP_ORCH, WS_ORCH } from "./configMinio.mjs";
import { HTTP_ORCH_AWS, WS_ORCH_AWS } from "./configAWS.mjs";

let HTTP_ORCH_USED, WS_ORCH_USED;

function parseArgs() {
  const orchIndex = process.argv.indexOf("--orch");
  if (orchIndex !== -1 && process.argv.length > orchIndex + 1) {
    return process.argv[orchIndex + 1];
  }
  return null;
}

function obtainOrchArg() {
  const orchType = parseArgs();

  if (orchType !== "aws" && orchType !== "local") {
    console.error(
      "❌ Missing or invalid orchestrator type. Use '--orch aws' or '--orch local'."
    );
    process.exit(1);
  }
  HTTP_ORCH_USED = orchType === "aws" ? HTTP_ORCH_AWS : HTTP_ORCH;
  WS_ORCH_USED = orchType === "aws" ? WS_ORCH_AWS : WS_ORCH;
}

(async () => {
  try {
    obtainOrchArg();
    await initPyodide();
    await registerWorker(HTTP_ORCH_USED, WS_ORCH_USED);
  } catch (e) {
    console.error("❌ Fatal error:", e.message);
    process.exit(1);
  }
})();
