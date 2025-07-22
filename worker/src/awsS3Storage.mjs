import {
  S3Client,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { STORAGE_ORCH } from "./configAWS.mjs";

let s3Client;

export function createAwsS3Client() {
  s3Client = new S3Client({
    region: "eu-north-1",
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
  });
}

export function getAwsS3Client() {
  if (!s3Client) throw new Error("AWS S3 client not initialized");
  return s3Client;
}

export function obtainBucketAndObjectName(fileUrl) {
  const parsed = new URL(fileUrl);
  const bucket = parsed.hostname.split(".")[0]; // bucket from URL like bucket.s3.region.amazonaws.com
  const objectName = parsed.pathname.slice(1); // remove leading /
  return { bucket, objectName };
}

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
  const { bucket } = obtainBucketAndObjectName(STORAGE_ORCH);
  const client = getAwsS3Client();

  // AWS S3 no necesita crear buckets si ya existen, pero puedes usar createBucket si quieres
  // Aquí asumimos bucket existe

  if (Array.isArray(result)) {
    const urls = [];
    for (let i = 0; i < result.length; i++) {
      const objectName = `${task.taskId}/${task.numWorker}-${i}.txt`;
      await client.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: objectName,
          Body: Buffer.from(result[i]),
          ContentType: "application/json",
        })
      );
      urls.push(`${STORAGE_ORCH}/${task.taskId}/${task.numWorker}-${i}.txt`);
    }
    return urls;
  } else {
    const objectName = `${task.taskId}/${task.numWorker || 0}.txt`;
    await client.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: objectName,
        Body: Buffer.from(result),
        ContentType: "application/json",
      })
    );
    return `${STORAGE_ORCH}/${task.taskId}/${task.numWorker || 0}.txt`;
  }
}
