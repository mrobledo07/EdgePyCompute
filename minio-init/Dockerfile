FROM alpine

RUN apk add --no-cache curl bash

# Instala mc (MinIO Client)
RUN curl -O https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc && \
    mv mc /usr/local/bin/

WORKDIR /minio-init

COPY entrypoint.sh /minio-init/entrypoint.sh
COPY example.txt /minio-init/example.txt

RUN chmod +x /minio-init/entrypoint.sh

ENTRYPOINT ["/minio-init/entrypoint.sh"]