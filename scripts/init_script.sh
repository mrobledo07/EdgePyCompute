#!/bin/bash

S3_BUCKET="pyedgecompute-bucket" # Replace with bucket name
OBJECT_KEY="instance-greetings-$(date +%Y%m%d-%H%M%S).txt"
CONTENT="Salutacions"

echo "$CONTENT" > /tmp/my-initial-file.txt

/usr/bin/aws s3 cp /tmp/my-initial-file.txt s3://"$S3_BUCKET"/"$OBJECT_KEY"

echo "S3 object written: s3://$S3_BUCKET/$OBJECT_KEY" >> /var/log/cloud-init-output.log