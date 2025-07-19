import * as Minio from "minio";
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
