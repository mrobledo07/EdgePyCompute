import {
  S3Client,
  HeadBucketCommand,
  ListObjectsV2Command,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import "dotenv/config";

export function createAwsS3Client() {
  return new S3Client({
    region: "eu-north-1",
    credentials: {
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
      sessionToken: process.env.AWS_SESSION_TOKEN,
    },
  });
}

async function checkBucketAccess(bucketName) {
  const client = createAwsS3Client();

  try {
    const response = await client.send(
      new GetObjectCommand({ Bucket: bucketName, Key: "example1.txt" })
    );
    console.log("RESPONSE: ", response);

    // Intentar comprobar si el bucket existe y accesible
    await client.send(new HeadBucketCommand({ Bucket: bucketName }));
    console.log(`✅ Bucket "${bucketName}" is accessible`);

    // Opcional: listar objetos para comprobar permisos de lectura
    const listResponse = await client.send(
      new ListObjectsV2Command({ Bucket: bucketName, MaxKeys: 1 })
    );
    console.log(
      `✅ List objects success. Found ${listResponse.KeyCount} object(s).`
    );

    return true;
  } catch (error) {
    console.error(`❌ Error accessing bucket "${bucketName}":`, error);
    return false;
  }
}

checkBucketAccess("clientbucketforstoringinputs");
