#!/bin/sh

set -e

# Wait for MinIO to be available
echo "⏳ Waiting for MinIO to become available..."
until curl -s http://minio-orchestrator:9000/minio/health/ready; do
    # echo "hoola"
    sleep 1
done

echo "✅ MinIO is ready, proceeding..."

exec node --watch src/main.mjs