#!/bin/sh
set -e

MINIO_ROOT_USER=${MINIO_ROOT_USER:-testkey}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-testpassword}
MINIO_BUCKET=${MINIO_BUCKET:-localbucket}

until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
  sleep 2
done

mc mb -p "local/${MINIO_BUCKET}" >/dev/null 2>&1 || true
mc anonymous set download "local/${MINIO_BUCKET}" >/dev/null 2>&1 || true

echo "MinIO bucket ready: ${MINIO_BUCKET}"
