// utils.js
import fs from "fs/promises";

export async function fileExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

export function parseArgs() {
  const args = process.argv.slice(2);
  const result = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--config") {
      result.configPath = args[i + 1];
      i++;
    } else if (args[i] === "--orch") {
      result.orchestrator = args[i + 1];
      i++;
    }
  }
  return result;
}

export function getOrchestratorUrls(input) {
  input = input.trim();
  if (!input.startsWith("http://") && !input.startsWith("https://")) {
    input = "http://" + input;
  }

  let url;
  try {
    url = new URL(input);
  } catch {
    throw new Error("Invalid orchestrator URL");
  }

  const hostRegex = /^([a-zA-Z0-9.-]+|\d{1,3}(\.\d{1,3}){3}):\d{1,5}$/;
  if (!hostRegex.test(url.host)) {
    throw new Error("Invalid host format. Expected: <domain|ip>:port");
  }

  return {
    http: url.protocol + "//" + url.host,
    ws: url.protocol.replace("http", "ws") + "//" + url.host,
  };
}
