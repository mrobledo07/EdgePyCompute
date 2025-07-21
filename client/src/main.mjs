// client.js
import { parseArgs, getOrchestratorUrls } from "./utils.mjs";
import { loadConfig, loadCode } from "./configLoader.mjs";
import {
  sendTaskWithRetry,
  connectToWebSocket,
  //connectClient,
} from "./orchestratorAPI.mjs";
import { Stopwatch } from "./stopWatch.mjs";

async function main() {
  const { configPath, orchestrator, client_id } = parseArgs();

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

  if (client_id) {
    console.log("FUNCTIONALITY NOT IMPLEMENTED YET");
    return;
    // console.log("🔌 Using provided client ID:", client_id);
    // try {
    //   const res = await connectClient(client_id, urls.http);
    //   connectToWebSocket(urls.ws, client_id);
    // } catch (err) {}
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

  const clientId = await sendTaskWithRetry(task, urls.http);
  let stopwatches = [];
  for (let i = 0; i < maxTasks; i++) {
    const stopwatch = new Stopwatch();
    stopwatch.start();
    stopwatches.push(stopwatch);
  }
  console.log("🕒 Stopwatch started for task:", clientId);
  const sentTime = Date.now() / 1000; // Convert to seconds
  connectToWebSocket(urls.ws, clientId, maxTasks, stopwatches, sentTime);
}

main();
