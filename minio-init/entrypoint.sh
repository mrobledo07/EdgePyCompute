#!/bin/sh

set -e

# Wait for MinIO to be available
echo "⏳ Waiting for MinIO to become available..."
until curl -s http://minio:9000/minio/health/ready; do
  sleep 1
done

echo "✅ MinIO is ready, proceeding..."

# Set the client alias
mc alias set myminio http://minio:9000 minioadmin minioadmin

# Create the bucket if it doesn't exist
mc mb myminio/test || echo "🪣 Bucket already exists"

# Upload the file
mc cp /minio-init/example1.txt myminio/test/
mc cp /minio-init/example2.txt myminio/test/
mc cp /minio-init/example3.txt myminio/test/
mc cp /minio-init/example4.txt myminio/test/
echo "📤 examples txt uploaded to the 'test' bucket"
