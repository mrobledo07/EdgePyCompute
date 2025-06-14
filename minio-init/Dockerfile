FROM alpine

RUN apk add --no-cache curl bash

# Instala mc (MinIO Client)
RUN curl -O https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc && \
    mv mc /usr/local/bin/

WORKDIR /minio-init

COPY entrypoint.sh /minio-init/entrypoint.sh
COPY example1.txt /minio-init/example1.txt
COPY example2.txt /minio-init/example2.txt
COPY example3.txt /minio-init/example3.txt
COPY example4.txt /minio-init/example4.txt

RUN chmod +x /minio-init/entrypoint.sh

ENTRYPOINT ["/minio-init/entrypoint.sh"]