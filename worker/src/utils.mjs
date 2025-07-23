function parseArgs() {
  const args = process.argv.slice(2);
  const result = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i] === "--orch") {
      result.orch = args[i + 1];
      i++;
    } else if (args[i] === "--storage") {
      result.storage = args[i + 1];
      i++;
    }
  }
  return result;
}

function convertHttpToWs(urlStr) {
  const url = new URL(urlStr);
  url.protocol = "ws";
  return url;
}

function isValidURL(url) {
  try {
    const newurl = new URL(url);
    if (newurl.protocol !== "http:" && newurl.protocol !== "https:")
      throw new Error();
    return true;
  } catch (_) {
    return false;
  }
}

function isValidStorage(urlstorage) {
  try {
    const newurl = new URL(urlstorage);
    if (
      newurl.protocol !== "http:" &&
      newurl.protocol !== "https:" &&
      newurl.protocol !== "s3:"
    )
      throw new Error();
    return true;
  } catch (_) {
    return false;
  }
}

function getOrchestratorUrls(input) {
  if (!isValidURL(input)) {
    console.error("Invalid orchestrator URL");
    process.exit(1);
  }

  const url = new URL(input);

  const hostRegex = /^([a-zA-Z0-9.-]+|\d{1,3}(\.\d{1,3}){3}):\d{1,5}$/;
  if (!hostRegex.test(url.host)) {
    throw new Error("Invalid host format. Expected: <domain|ip>:port");
  }

  let ws = convertHttpToWs(url.toString());
  console.log("WS:", ws);
  return {
    http: url.protocol + "//" + url.host,
    ws: ws.protocol + "//" + ws.host,
  };
}

function processStorage(url) {
  if (!isValidStorage(url)) {
    console.error("❌ Invalid '--storage' URL.");
    process.exit(1);
  }
  const newurl = new URL(url);
  return newurl.protocol + "//" + newurl.host;
}

export function obtainArgs(CONFIG) {
  const { orch, storage } = parseArgs();

  if (!orch || !storage) {
    console.error("❌ Missing '--orch' or '--storage' URL.");
    process.exit(1);
  }

  let urls;
  try {
    urls = getOrchestratorUrls(orch);
  } catch (err) {
    console.error("❌", err.message);
    process.exit(1);
  }

  const store = processStorage(storage);
  CONFIG.STORAGE = store;
  CONFIG.WS_ORCH = urls.ws;
  CONFIG.HTTP_ORCH = urls.http;
  // Debug output (optional)
  console.log("✅ HTTP_ORCH_USED:", CONFIG.HTTP_ORCH);
  console.log("✅ WS_ORCH_USED:", CONFIG.WS_ORCH);
  console.log("✅ STORAGE_ORCH_USED:", CONFIG.STORAGE);
}
