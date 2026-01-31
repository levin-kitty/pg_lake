#!/bin/bash
set -e

echo "=== pg_lake devcontainer setup ==="

# Add PostgreSQL and vcpkg to PATH permanently
if ! grep -q "pgsql-18/bin" ~/.bashrc 2>/dev/null; then
    cat >> ~/.bashrc << 'EOF'

# pg_lake environment
export PATH="/home/postgres/pgsql-18/bin:/home/postgres/vcpkg:$PATH"
export VCPKG_TOOLCHAIN_PATH="/home/postgres/vcpkg/scripts/buildsystems/vcpkg.cmake"
export PGDATA="/home/postgres/pgsql-18/data"
export PG_MAJOR=18
EOF
    echo "Added pg_lake environment to ~/.bashrc"
fi

# Source it for current session
export PATH="/home/postgres/pgsql-18/bin:/home/postgres/vcpkg:$PATH"
export VCPKG_TOOLCHAIN_PATH="/home/postgres/vcpkg/scripts/buildsystems/vcpkg.cmake"
export PGDATA="/home/postgres/pgsql-18/data"
export PG_MAJOR=18

# Fix volume permissions (Docker creates as root)
if [ -d "$PGDATA" ] && [ "$(stat -c '%U' $PGDATA 2>/dev/null)" = "root" ]; then
    echo "Fixing $PGDATA permissions..."
    sudo chown -R postgres:postgres "$PGDATA"
fi

# Initialize PostgreSQL data directory if not exists
if [ ! -f "$PGDATA/PG_VERSION" ]; then
    echo "Initializing PostgreSQL data directory..."
    initdb -D "$PGDATA" -U postgres --locale=C.UTF-8 --data-checksums

    # Configure postgresql.conf
    echo "shared_preload_libraries = 'pg_extension_base'" >> "$PGDATA/postgresql.conf"
    echo "log_statement = 'all'" >> "$PGDATA/postgresql.conf"
fi

# Create MinIO secret setup script for pgduck_server
cat > ~/setup_minio_secret.sql << 'EOF'
CREATE SECRET IF NOT EXISTS minio_secret (
    TYPE S3,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'minio:9000',
    SCOPE 's3://test-bucket',
    URL_STYLE 'path',
    USE_SSL false
);
EOF
echo "Created ~/setup_minio_secret.sql for pgduck_server"

# Install pipenv dependencies for tests
echo "Installing Python test dependencies..."
cd /home/postgres/pg_lake
pipenv install --dev 2>/dev/null || true

# Install Claude Code
echo "Installing Claude Code..."
npm install -g @anthropic-ai/claude-code 2>/dev/null || true

echo ""
echo "=== Setup complete ==="
echo ""
echo "Quick start:"
echo "  1. Build pg_lake:     make install      (first time)"
echo "                        make install-fast (subsequent)"
echo ""
echo "  2. Start PostgreSQL:  pg_ctl -D \$PGDATA -l ~/logfile start"
echo ""
echo "  3. Start pgduck:      pgduck_server --cache_dir /tmp/cache \\"
echo "                          --init_file_path ~/setup_minio_secret.sql &"
echo ""
echo "  4. Connect:           psql -U postgres"
echo ""
echo "  5. Run tests:         make check"
echo ""
echo "  6. Claude Code:       claude"
echo ""
echo "  7. MinIO Console:     http://localhost:9001 (minioadmin/minioadmin)"
echo ""
