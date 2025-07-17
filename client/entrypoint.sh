#!/bin/sh

set -e

# Wait for MinIO to be available
echo "⏳ Waiting for MinIO to become available..."
until curl -s http://minio-client:9000/minio/health/ready; do
  sleep 1
done

echo "✅ MinIO is ready, proceeding..."

# Set the client alias
mc alias set myminio http://minio-client:9000 minioadmin minioadmin

# Check if bucket exists
if mc ls myminio/test > /dev/null 2>&1; then
  echo "🪣 Bucket 'test' already exists"
else
  mc mb myminio/test
  echo "🪣 Bucket 'test' created"
fi

# Esperar hasta que el bucket esté disponible de verdad
until mc ls myminio/test > /dev/null 2>&1; do
  echo "⏳ Waiting for bucket to become fully available..."
  sleep 1
done

# Upload the file
mc cp files/example1.txt myminio/test/
mc cp files/example2.txt myminio/test/
mc cp files/example3.txt myminio/test/
mc cp files/example4.txt myminio/test/
mc cp files/terasort-20m myminio/test/
echo "📤 examples txt uploaded to the 'test' bucket"

exec node --watch client.mjs
