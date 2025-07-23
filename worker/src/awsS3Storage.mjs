import {
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import {
  createAwsS3Client,
  getAwsS3Client,
  obtainBucketAndObjectName,
  obtainBucketName,
} from "./awsS3Client.mjs";
//import { STORAGE_ORCH } from "./configAWS.mjs";
import { CONFIG } from "./main.mjs";

export async function getTextFromS3(fileUrl, offset = -1, numMappers = -1) {
  createAwsS3Client();
  const { bucket, objectName } = obtainBucketAndObjectName(fileUrl);
  const client = getAwsS3Client();

  if (offset === -1) {
    // Get full object
    const command = new GetObjectCommand({ Bucket: bucket, Key: objectName });
    const response = await client.send(command);
    return streamToBuffer(response.Body);
  } else {
    // Get partial object (range)
    const headCmd = new HeadObjectCommand({ Bucket: bucket, Key: objectName });
    const metadata = await client.send(headCmd);
    const totalSize = metadata.ContentLength;
    const chunkSize = Math.floor(totalSize / numMappers);
    const start = offset * chunkSize;
    let end = (offset + 1) * chunkSize - 1;
    if (offset === numMappers - 1) end = totalSize - 1;

    const range = `bytes=${start}-${end}`;
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: objectName,
      Range: range,
    });
    const response = await client.send(command);
    return streamToBuffer(response.Body);
  }
}

async function streamToBuffer(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("end", () => resolve(Buffer.concat(chunks)));
    stream.on("error", reject);
  });
}

export async function getPartialObjectS3(task) {
  const offset = task.numWorker;
  const numMappers = task.numMappers;
  if (offset === undefined || numMappers === undefined) {
    throw new Error("Missing offset or numMappers");
  }
  const text = await getTextFromS3(task.arg, offset, numMappers);
  return text;
}

export async function getSerializedResults(results) {
  if (!Array.isArray(results) || results.length === 0)
    throw new Error("Invalid results array");
  const b64List = [];
  for (const result of results) {
    let partialResult = await getTextFromS3(result);
    b64List.push(partialResult.toString("utf-8"));
  }
  return b64List;
}

export async function setSerializedResult(task, result) {
  createAwsS3Client();

  // Use STORAGE_ORCH as base URL, assuming like "https://bucket.s3.region.amazonaws.com"
  const bucket = obtainBucketName(CONFIG.STORAGE);
  const client = getAwsS3Client();

  let cleanedTaskId = task.taskId;
  let removedPart = "";

  // Regex para mapreduce o reduce al final
  if (
    task.type === "mapterasort" ||
    task.type === "mapwordcount" ||
    task.type === "reduceterasort" ||
    task.type === "reducewordcount"
  ) {
    // Busca y captura la parte final -mapperX o -reducerX (X puede ser dígito o cualquier cadena)
    const regex = /-(mapper\d+|reducer[\w\d]*)$/;
    const match = task.taskId.match(regex);

    if (match) {
      removedPart = match[1]; // la parte que se eliminó (mapperX o reducerX)
      cleanedTaskId = task.taskId.replace(regex, "");
    }
  }

  if (Array.isArray(result)) {
    const urls = [];
    for (let i = 0; i < result.length; i++) {
      // Construimos el path con cleanedTaskId / removedPart / task.numWorker-i.txt
      const objectName = `${task.clientId}/${cleanedTaskId}/${removedPart}/${
        task.numWorker || 0
      }-${i}.txt`;

      await client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: objectName,
          Body: Buffer.from(result[i]),
          ContentType: "application/json",
        })
      );

      urls.push(`${CONFIG.STORAGE}/${objectName}`);
    }
    return urls;
  } else {
    const basePath = `${task.clientId}/${cleanedTaskId}`;
    const finalPath = removedPart ? `${basePath}/${removedPart}` : basePath;

    const objectName = `${finalPath}/${task.numWorker || 0}.txt`;

    await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: objectName,
        Body: Buffer.from(result),
        ContentType: "application/json",
      })
    );

    return `${CONFIG.STORAGE}/${objectName}`;
  }
}
