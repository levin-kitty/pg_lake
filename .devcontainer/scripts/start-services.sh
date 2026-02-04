#!/usr/bin/env bash
set -euo pipefail

PGBASEDIR=${PGBASEDIR:-/home/postgres}
PG_MAJOR=${PG_MAJOR:-17}
DATA_DIR="${PGBASEDIR}/pgsql-${PG_MAJOR}/data"
SOCKET_DIR="${PGBASEDIR}/pgduck_socket_dir"

MINIO_ENDPOINT=${MINIO_ENDPOINT:-http://minio:9000}
MINIO_BUCKET=${MINIO_BUCKET:-localbucket}
MINIO_ROOT_USER=${MINIO_ROOT_USER:-testkey}
MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD:-testpassword}
PG_LAKE_S3_PREFIX=${PG_LAKE_S3_PREFIX:-s3://${MINIO_BUCKET}/pg_lake/}
SKIP_PGDUCK_SERVER=${SKIP_PGDUCK_SERVER:-0}

sudo mkdir -p "${SOCKET_DIR}"
sudo mkdir -p "${DATA_DIR}"
sudo mkdir -p "${PGBASEDIR}/cache"
sudo chown -R postgres:postgres "${SOCKET_DIR}" "${DATA_DIR}" "${PGBASEDIR}/cache"

if [ ! -f "${DATA_DIR}/PG_VERSION" ]; then
  initdb -D "${DATA_DIR}" -U postgres --locale=C.UTF-8 --data-checksums
fi

mkdir -p "${DATA_DIR}/base/pgsql_tmp"
chmod 700 "${DATA_DIR}/base/pgsql_tmp"

# Ensure dev config is included
CONF_FILE="${DATA_DIR}/postgresql.conf"
DEV_CONF_FILE="${DATA_DIR}/pg_lake_dev.conf"
if ! grep -q "pg_lake_dev.conf" "${CONF_FILE}"; then
  echo "include_if_exists = '${DEV_CONF_FILE}'" >> "${CONF_FILE}"
fi

PRELOAD_LIB=""
if [ -f "${PGBASEDIR}/pgsql-${PG_MAJOR}/lib/pg_extension_base.so" ]; then
  PRELOAD_LIB="pg_extension_base"
fi

cat > "${DEV_CONF_FILE}" <<EOF_CONF
listen_addresses = '*'
port = 5432
EOF_CONF

if [ -n "${PRELOAD_LIB}" ]; then
  echo "shared_preload_libraries = '${PRELOAD_LIB}'" >> "${DEV_CONF_FILE}"
fi

if [ -f "${PGBASEDIR}/pgsql-${PG_MAJOR}/share/extension/pg_lake_iceberg.control" ]; then
  echo "pg_lake_iceberg.default_location_prefix = '${PG_LAKE_S3_PREFIX}'" >> "${DEV_CONF_FILE}"
fi

if [ -f "${PGBASEDIR}/pgsql-${PG_MAJOR}/share/extension/pg_lake_engine.control" ]; then
  echo "pg_lake_engine.host = 'host=${SOCKET_DIR} port=5332'" >> "${DEV_CONF_FILE}"
fi

cat > "${DATA_DIR}/pg_hba.conf" <<'EOF_HBA'
local all all trust
host all all 127.0.0.1/32 trust
host all all ::1/128 trust
host all all 0.0.0.0/0 trust
EOF_HBA

if ! pg_ctl -D "${DATA_DIR}" status >/dev/null 2>&1; then
  pg_ctl -D "${DATA_DIR}" -l "${DATA_DIR}/logfile" -w start
fi

# Ensure pg_extension_base is loaded when installed (requires restart)
if [ -f "${PGBASEDIR}/pgsql-${PG_MAJOR}/lib/pg_extension_base.so" ]; then
  current_preload="$(psql -U postgres -d postgres -Atc 'SHOW shared_preload_libraries;' 2>/dev/null || true)"
  if ! echo "${current_preload}" | grep -q "pg_extension_base"; then
    pg_ctl -D "${DATA_DIR}" -m fast stop
    pg_ctl -D "${DATA_DIR}" -l "${DATA_DIR}/logfile" -w start
  fi
fi

# Start pgduck_server if installed
if [ "${SKIP_PGDUCK_SERVER}" != "1" ] && command -v pgduck_server >/dev/null 2>&1; then
  INIT_SQL="${DATA_DIR}/pgduck_init.sql"
  cat > "${INIT_SQL}" <<EOF_SQL
CREATE SECRET s3_minio (
  TYPE S3,
  KEY_ID '${MINIO_ROOT_USER}',
  SECRET '${MINIO_ROOT_PASSWORD}',
  ENDPOINT 'minio:9000',
  SCOPE 's3://${MINIO_BUCKET}',
  URL_STYLE 'path',
  USE_SSL false,
  REGION 'us-east-1'
);
EOF_SQL

  if ! pgrep -f "pgduck_server.*--port 5332" >/dev/null 2>&1; then
    nohup pgduck_server \
      --cache_dir "${PGBASEDIR}/cache" \
      --unix_socket_directory "${SOCKET_DIR}" \
      --unix_socket_group postgres \
      --port 5332 \
      --init_file_path "${INIT_SQL}" \
      > "${DATA_DIR}/pgduck_server.log" 2>&1 &
  fi
elif [ "${SKIP_PGDUCK_SERVER}" != "1" ]; then
  echo "pgduck_server not found. Build and install with: make install-pgduck_server"
else
  echo "SKIP_PGDUCK_SERVER=1 set; skipping pgduck_server startup"
fi

# Create pg_lake extensions if installed
if [ -f "${PGBASEDIR}/pgsql-${PG_MAJOR}/share/extension/pg_lake.control" ]; then
  psql -U postgres -d postgres -c "CREATE EXTENSION IF NOT EXISTS pg_lake CASCADE;" || true
fi
