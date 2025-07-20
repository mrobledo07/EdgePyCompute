import {
  getTextFromMinio,
  getPartialObjectMinio,
  getSerializedResults,
  setSerializedResult,
} from "./minioStorage.mjs";
import { getPyodide } from "./pyodideRuntime.mjs";

export async function executeTask(task, ws, stopWatch) {
  const pyodide = getPyodide();
  stopWatch.start();
  try {
    console.log("RECEIVING CLIENT ID:", task.clientId);
    let bytes;
    if (task.type === "mapwordcount" || task.type === "mapterasort") {
      console.log(
        `🔍 Getting partial object for MAPPER task ${task.arg}:${task.taskId}`
      );
      //
      bytes = await getPartialObjectMinio(task);
    } else if (
      task.type === "reducewordcount" ||
      task.type === "reduceterasort"
    ) {
      console.log(
        `🔍 Getting serialized results for REDUCER task ${task.arg}:${task.taskId}`
      );
      bytes = await getSerializedResults(task.arg);
    } else {
      console.log(
        `🔍 Getting full object for task ${task.arg}:${task.taskId} (not
        map or reduce)`
      );
      bytes = await getTextFromMinio(task.arg);
    }

    let rawBytesLine;
    if (task.type === "reduceterasort" || task.type === "reducewordcount") {
      // Reducer: arg es un JSON‐string con ["b64part1","b64part2",...]
      // Pasamos esa cadena TEXTUAL directamente a Python
      rawBytesLine = `raw_bytes = ${JSON.stringify(bytes)}`;
      console.log(
        `🔍 raw_bytes for REDUCER task ${task.taskId} is: ${rawBytesLine}`
      );
    } else {
      // Map: bytes es un Buffer → lo pasamos como Base64 y DECODIFICAMOS en Python
      const b64 = bytes.toString("base64");
      rawBytesLine = `raw_bytes = base64.b64decode("${b64}")`;
    }

    const roundTo4 = (num) => Math.round(num * 10000) / 10000;

    stopWatch.stop();
    //let ioTime = parseFloat(stopWatch.getDuration().toFixed(4));
    let ioTime = roundTo4(stopWatch.getDuration());

    const pyScript = `
${task.code}
${rawBytesLine}
try:
    result = task(raw_bytes)
except Exception as e:
    result = str(e)
result
      `;
    // console.log(
    //   `📜 Executing task task ${task.taskId} from client ${task.clientId} with arg ${task.arg} and with code:\n${pyScript}`
    // );
    console.log(
      `📜 Executing task ${task.taskId} from client ${task.clientId} with arg ${task.arg}`
    );
    //sleep for 3 seconds to simulate a long task
    // await new Promise((resolve) => setTimeout(resolve, 3000));

    stopWatch.start();

    let result = await pyodide.runPythonAsync(pyScript);
    // console.log(
    //   `✔️ Completed task ${task.taskId} from client ${task.clientId} with arg ${task.arg}`,
    //   result
    // );
    stopWatch.stop();
    //const cpuTime = parseFloat(stopWatch.getDuration().toFixed(4));
    const cpuTime = roundTo4(stopWatch.getDuration());

    console.log(
      `✔️ Completed task ${task.taskId} from client ${task.clientId} with arg ${task.arg}`
    );

    if (task.type === "mapterasort") {
      result = JSON.parse(result);
    }

    //console.log("🔍 typeof result:", typeof result);
    //console.log("🔍 instanceof Array:", result instanceof Array);
    // console.log("🔍 isPyProxy:", isPyProxy(result));

    stopWatch.start();
    const resultURL = await setSerializedResult(task, result);
    stopWatch.stop();
    //ioTime += parseFloat(stopWatch.getDuration().toFixed(4));
    ioTime = roundTo4(ioTime + stopWatch.getDuration());

    // Create resultUrl
    ws.send(
      JSON.stringify({
        clientId: task.clientId,
        taskId: task.taskId,
        status: "done",
        result: resultURL,
        ioTime,
        cpuTime,
      })
    );
  } catch (e) {
    console.error(
      `❌ Error on task ${task.taskId} from client ${task.clientId} with arg ${task.arg}`,
      e.message
    );
    console.error(
      `❌ Error on task ${task.taskId} from client ${task.clientId} with arg ${task.arg}`,
      e
    );
    ws.send(
      JSON.stringify({
        clientId: task.clientId,
        taskId: task.taskId,
        status: "error",
        result: e.message,
        ioTime,
        cpuTime,
      })
    );
    stopWatch.stop();
  }
}
