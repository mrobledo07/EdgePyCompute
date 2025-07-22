import {
  getMinioClient,
  createMinioClient,
  obtainBucketAndObjectName,
} from "./minioClient.mjs";

import { STORAGE_ORCH } from "./configMinio.mjs";

export async function getTextFromMinio(fileUrl, offset = -1, numMappers = -1) {
  createMinioClient(fileUrl);
  const minioClient = getMinioClient();
  const { bucket, objectName } = obtainBucketAndObjectName(fileUrl);
  let stream;
  if (offset == -1) {
    stream = await minioClient.getObject(bucket, objectName);
  } else {
    // If offset is provided, get a partial object
    const stat = await minioClient.statObject(bucket, objectName);
    const totalSize = stat.size;
    const chunkSize = Math.floor(totalSize / numMappers);
    const start = offset * chunkSize;
    let end = (offset + 1) * chunkSize - 1;

    // Assign the last chunk to the last mapper
    if (offset === numMappers - 1) end = totalSize - 1;

    stream = await minioClient.getPartialObject(bucket, objectName, start, end);
  }
  return new Promise((res, rej) => {
    const chunks = [];
    stream.on("data", (c) => chunks.push(c));
    stream.on("end", () => res(Buffer.concat(chunks)));
    stream.on("error", (e) => rej(e));
  });
}

export async function getPartialObjectMinio(task) {
  const offset = task.numWorker;
  const numMappers = task.numMappers;
  console.log(
    `🔍 Getting partial object from Minio: offset=${offset}, numMappers=${numMappers}`
  );
  if (offset === undefined || numMappers === undefined) {
    throw new Error(
      "Invalid task received. Missing offset or num of mappers for MAPPER partial object."
    );
  }
  console.log(`🔍 Getting TASK ARG ${task.arg}`);
  const text = await getTextFromMinio(task.arg, offset, numMappers);
  return text;
}

export async function getSerializedResults(results) {
  if (!Array.isArray(results) || results.length === 0)
    throw new Error(
      "Invalid task received. Missing array of results for REDUCER."
    );

  const b64List = [];
  for (const result of results) {
    let partialResult = await getTextFromMinio(result);
    console.log("🔍 First partial:", partialResult);
    b64List.push(partialResult.toString("utf-8"));
  }
  //console.log("🔍 Mappers results aggregated:", resultJSON);
  console.log("🔍 Returning serialized results for REDUCER:", b64List);
  // Return the
  return b64List;
}

export async function setSerializedResult(task, result) {
  // Construimos la basePath con clientId y taskId original (se usará en URL de retorno)
  const basePath = `${STORAGE_ORCH}/${task.clientId}/${task.taskId}`;

  createMinioClient(basePath);
  const minioClient = getMinioClient();
  const { bucket } = obtainBucketAndObjectName(basePath);

  // Variables para taskId limpio y la parte removida
  let cleanedTaskId = task.taskId;
  let removedPart = "";

  if (
    task.type === "mapterasort" ||
    task.type === "mapwordcount" ||
    task.type === "reduceterasort" ||
    task.type === "reducewordcount"
  ) {
    const regex = /-(mapper\d+|reducer[\w\d]*)$/;
    const match = task.taskId.match(regex);
    if (match) {
      removedPart = match[1];
      cleanedTaskId = task.taskId.replace(regex, "");
    }
  }

  try {
    await minioClient.makeBucket(bucket, "us-east-1");
  } catch (e) {
    if (e.code !== "BucketAlreadyOwnedByYou") {
      console.error(`❌ Error creating bucket ${bucket}:`, e.message);
      throw e;
    }
    console.log(`ℹ️ Bucket ${bucket} already exists, skipping creation.`);
  }

  if (Array.isArray(result)) {
    console.log("WE ARE IN REDUCER TERASORT ARRAY");
    const urls = [];
    for (let i = 0; i < result.length; i++) {
      const reducerResult = result[i];
      // Construimos el objectName con cleanedTaskId, removedPart y numWorker-i
      const objectName = `${task.clientId}/${cleanedTaskId}/${removedPart}/${
        task.numWorker || 0
      }-${i}.txt`;

      console.log(
        `📦 Storing reducer result in Minio: ${basePath}/${objectName}`
      );
      try {
        await minioClient.putObject(
          bucket,
          objectName,
          Buffer.from(reducerResult),
          reducerResult.length,
          "application/json"
        );
        urls.push(`${basePath}/${objectName}`);
      } catch (e) {
        console.error(`❌ Error storing reducer result [${i}]:`, e.message);
        throw e;
      }
    }
    return urls;
  } else {
    console.log("WE ARE WHERE WE SHOULD NOT BE");
    const basePath = `${task.clientId}/${cleanedTaskId}`;
    const finalPath = removedPart ? `${basePath}/${removedPart}` : basePath;

    const objectName = `${finalPath}/${task.numWorker || 0}.txt`;

    console.log(`📦 Storing result in Minio: ${basePath}/${objectName}`);
    try {
      await minioClient.putObject(
        bucket,
        objectName,
        Buffer.from(result),
        result.length,
        "application/json"
      );
      return `${basePath}/${objectName}`;
    } catch (e) {
      console.error(`❌ Error storing result in Minio:`, e.message);
      throw e;
    }
  }
}
