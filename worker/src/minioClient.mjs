import * as Minio from "minio";
import "dotenv/config";
let minioClient;

export function createMinioClient(fileURL) {
  const parsed = new URL(fileURL);
  minioClient = new Minio.Client({
    endPoint: parsed.hostname,
    port: parseInt(parsed.port),
    useSSL: false,
    accessKey: "minioadmin",
    secretKey: "minioadmin",
  });
}

export function createMinioClientS3() {
  minioClient = new Minio.Client({
    endPoint: "s3.eu-north-1.amazonaws.com",
    accessKey: process.env.AWS_ACCESS_KEY_ID,
    secretKey: process.env.AWS_SECRET_ACCESS_KEY,
    useSSL: true,
  });
}

export function getMinioClient() {
  if (!minioClient) throw new Error("MinIO client not initialized");
  return minioClient;
}

export function obtainBucketAndObjectName(fileUrl) {
  const parsed = new URL(fileUrl);
  const [, bucket, ...rest] = parsed.pathname.split("/");
  const objectName = rest.join("/");
  return { bucket, objectName };
}

export function obtainBucketAndObjectNameS3(fileUrl) {
  const parsed = new URL(fileUrl);
  const bucket = parsed.hostname;
  const objectName = parsed.pathname.slice(1);
  return { bucket, objectName };
}
