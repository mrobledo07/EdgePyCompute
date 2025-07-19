import {
  getTextFromMinio,
  getPartialObjectMinio,
  getSerializedMappersResults,
  setSerializedMapperResult,
} from "./minioStorage.mjs";
import { getPyodide } from "./pyodideRuntime.mjs";

export async function executeTask(task, ws, workerId) {
  const pyodide = getPyodide();
  try {
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
      bytes = await getSerializedMappersResults(task.arg);
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
    } else {
      // Map: bytes es un Buffer → lo pasamos como Base64 y DECODIFICAMOS en Python
      const b64 = bytes.toString("base64");
      rawBytesLine = `raw_bytes = base64.b64decode("${b64}")`;
    }

    const pyScript = `
${task.code}
${rawBytesLine}
try:
    result = task(raw_bytes)
except Exception as e:
    result = str(e)
result
      `;
    console.log(
      `📜 Executing task ${task.arg}:${task.taskId} with code:\n${pyScript}`
    );
    //sleep for 3 seconds to simulate a long task
    // await new Promise((resolve) => setTimeout(resolve, 3000));
    let result = await pyodide.runPythonAsync(pyScript);
    console.log(`✔️ Completed ${task.arg}:${task.taskId}:`, result);

    if (task.type === "mapterasort") {
      result = JSON.parse(result);
    }

    console.log("🔍 typeof result:", typeof result);
    console.log("🔍 instanceof Array:", result instanceof Array);
    // console.log("🔍 isPyProxy:", isPyProxy(result));

    if (task.type === "mapwordcount" || task.type === "mapterasort") {
      const resultURL = await setSerializedMapperResult(task, result, workerId);
      // Create resultUrl
      ws.send(
        JSON.stringify({
          arg: task.arg,
          taskId: task.taskId,
          status: "done",
          result: resultURL,
        })
      );
    } else if (task.type === "reduceterasort") {
      /* Code if we want to change output of reduce terasort (now is pickle and base64)*/
      // result = Buffer.from(result, "base64").toString("utf-8"); <-- Only if result is a CSV
      // result = JSON.parse(result);
      ws.send(
        JSON.stringify({
          arg: task.arg,
          taskId: task.taskId,
          status: "done",
          result,
        })
      );
    } else {
      ws.send(
        JSON.stringify({
          arg: task.arg,
          taskId: task.taskId,
          status: "done",
          result,
        })
      );
    }
  } catch (e) {
    console.error(`❌ Error on ${task.arg}:${task.taskId}:`, e.message);
    console.error(`❌ Error on ${task.arg}:${task.taskId}:`, e);
    ws.send(
      JSON.stringify({
        arg: task.arg,
        taskId: task.taskId,
        status: "error",
        result: e.message,
      })
    );
  }
}
