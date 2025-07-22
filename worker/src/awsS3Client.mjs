import { S3Client } from "@aws-sdk/client-s3";
import "dotenv/config";

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
