import { S3Client } from "@aws-sdk/client-s3";
import "dotenv/config";

let s3Client;

export function createAwsS3Client() {
  s3Client = new S3Client({
    region: "eu-north-1",
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      sessionToken: process.env.AWS_SESSION_TOKEN,
    },
  });
}

export function getAwsS3Client() {
  if (!s3Client) throw new Error("AWS S3 client not initialized");
  return s3Client;
}

export function obtainBucketName(fileUrl) {
  if (!fileUrl.startsWith("s3://")) {
    throw new Error("Invalid S3 URL format: must start with 's3://'");
  }

  const withoutPrefix = fileUrl.slice("s3://".length);

  // Asegúrate de que no contenga una barra, porque eso implicaría que hay una ruta después del bucket
  if (withoutPrefix.includes("/")) {
    throw new Error("Expected only bucket name, but got a full S3 path");
  }

  const bucket = withoutPrefix.trim();

  if (!bucket) {
    throw new Error("Bucket name is empty");
  }
  console.log("OBTAINING BUCKET ORCHESTRATOR");
  console.log("BUCKET: ", bucket);

  return bucket;
}

export function obtainBucketAndObjectName(fileUrl) {
  if (fileUrl.startsWith("s3://")) {
    // Parse s3://bucket/objectName
    const withoutPrefix = fileUrl.slice("s3://".length); // "bucket/objectName"
    const slashIndex = withoutPrefix.indexOf("/");
    if (slashIndex === -1) {
      throw new Error("Invalid S3 URL, missing object key");
    }
    const bucket = withoutPrefix.slice(0, slashIndex);
    const objectName = withoutPrefix.slice(slashIndex + 1);
    console.log("STARTS WITH S3");
    console.log("BUCKET, ", bucket);
    console.log("OBJECTNAME, ", objectName);
    return { bucket, objectName };
  } else {
    throw new Error("Invalid S3 URL format: must start with 's3://'");
    // Parse https://bucket.s3.region.amazonaws.com/objectName
    // const parsed = new URL(fileUrl);
    // const bucket = parsed.hostname.split(".")[0];
    // const objectName = parsed.pathname.slice(1);
    // console.log("STARTS WITH bucket.s3.region.amazonaws.com/objectName");
    // console.log("BUCKET, ", bucket);
    // console.log("OBJECTNAME, ", objectName);
    // return { bucket, objectName };
  }
}
