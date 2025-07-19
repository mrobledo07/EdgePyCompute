// client.js
import { parseArgs, getOrchestratorUrls } from "./utils.mjs";
import { loadConfig, loadCode } from "./configLoader.mjs";
import { sendTaskWithRetry, connectToWebSocket } from "./orchestratorAPI.mjs";

async function main() {
  const { configPath, orchestrator } = parseArgs();

  if (!configPath || !orchestrator) {
    console.error("❌ Missing --config or --orch argument");
    process.exit(1);
  }

  let urls;
  try {
    urls = getOrchestratorUrls(orchestrator);
  } catch (err) {
    console.error("❌", err.message);
    process.exit(1);
  }

  let task, maxTasks;
  try {
    const config = await loadConfig(configPath);
    const code = await loadCode(config.code);
    maxTasks = config.args.length;
    task = {
      type: config.type,
      args: config.args,
      code: code,
    };
  } catch (err) {
    console.error("❌ Config error:", err.message);
    process.exit(1);
  }

  const taskId = await sendTaskWithRetry(task, urls.http);
  connectToWebSocket(urls.ws, taskId, maxTasks);
}

main();
