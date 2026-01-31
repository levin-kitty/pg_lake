# pg_lake Development Container

CLion, VS Code 등에서 사용할 수 있는 devcontainer 설정입니다.

## 포함된 것

- **PostgreSQL 16, 17, 18** (소스 빌드)
- **PostGIS 3.6**
- **vcpkg** (Azure SDK 등 의존성)
- **DuckDB** (pg_lake 빌드 시 컴파일)
- **MinIO** (S3 호환 스토리지, WebUI 포함)
- **Claude Code** (AI 코딩 어시스턴트)
- 개발 도구: `ps`, `htop`, `jq`, `vim`, `tree` 등

## 시작하기

### 1. 컨테이너 열기

CLion: 프로젝트 열면 `.devcontainer` 감지 → "Open in Container" 선택

VS Code: `Dev Containers: Reopen in Container` 명령 실행

### 2. pg_lake 빌드

```bash
# 첫 빌드 (DuckDB 컴파일 포함, 오래 걸림)
make install

# 이후 빌드 (DuckDB 재사용, 빠름)
make install-fast
```

### 3. PostgreSQL 초기화 (최초 1회)

```bash
# 데이터 디렉토리 권한 수정 (Docker 볼륨 이슈)
sudo chown -R postgres:postgres $PGDATA

# 데이터베이스 클러스터 초기화
initdb -D $PGDATA -U postgres --locale=C.UTF-8 --data-checksums

# shared_preload_libraries 설정
echo "shared_preload_libraries = 'pg_extension_base'" >> $PGDATA/postgresql.conf
```

### 4. 서버 시작

```bash
# PostgreSQL 시작
pg_ctl -D $PGDATA -l ~/logfile start

# pgduck_server 시작 (MinIO 시크릿 포함)
pgduck_server --cache_dir /tmp/cache --init_file_path ~/setup_minio_secret.sql &
```

### 5. 익스텐션 설치

```bash
psql -U postgres -c "CREATE EXTENSION pg_lake CASCADE;"
```

### 6. 테스트 실행

```bash
# 전체 테스트
make check

# 특정 컴포넌트 테스트
make check-pg_lake_table
make check-pg_lake_iceberg
```

## MinIO (S3 호환 스토리지)

devcontainer에 MinIO가 포함되어 있어 로컬에서 S3 테스트가 가능합니다.

| 항목 | 값 |
|------|-----|
| Console URL | http://localhost:9001 |
| API Endpoint | minio:9000 (컨테이너 내부) |
| Access Key | minioadmin |
| Secret Key | minioadmin |
| 기본 버킷 | test-bucket |

### PostgreSQL에서 MinIO 사용

```sql
-- Iceberg 테이블 기본 위치 설정
SET pg_lake_iceberg.default_location_prefix TO 's3://test-bucket/iceberg';

-- Iceberg 테이블 생성
CREATE TABLE my_table (id int, name text) USING iceberg;
INSERT INTO my_table VALUES (1, 'hello');

-- 데이터 확인
SELECT * FROM my_table;
```

### pgduck_server에서 직접 확인

```bash
psql -p 5332 -h /tmp -c "SELECT * FROM 's3://test-bucket/iceberg/**/*.parquet';"
```

## 환경 변수

| 변수 | 값 | 설명 |
|------|-----|------|
| `PG_MAJOR` | 18 | 사용할 PostgreSQL 버전 |
| `PGDATA` | /home/postgres/pgsql-18/data | 데이터 디렉토리 |
| `VCPKG_TOOLCHAIN_PATH` | /home/postgres/vcpkg/scripts/buildsystems/vcpkg.cmake | vcpkg 툴체인 |
| `MINIO_ENDPOINT` | minio:9000 | MinIO API 엔드포인트 |
| `MINIO_ACCESS_KEY` | minioadmin | MinIO 접근 키 |
| `MINIO_SECRET_KEY` | minioadmin | MinIO 비밀 키 |

## 다른 PostgreSQL 버전 사용

```bash
# PostgreSQL 17 사용
export PG_MAJOR=17
export PATH="/home/postgres/pgsql-17/bin:$PATH"
export PGDATA="/home/postgres/pgsql-17/data"
```

## 볼륨

| 볼륨 | 마운트 위치 | 용도 |
|------|-------------|------|
| `pg_lake-pgdata` | /home/postgres/pgsql-18/data | PostgreSQL 데이터 |
| `pg_lake-cache` | /tmp/cache | pgduck_server 캐시 |
| `minio-data` | /data (MinIO 컨테이너) | MinIO 오브젝트 스토리지 |

## 컨테이너 재생성 시

컨테이너를 Recreate하면 pg_lake 빌드 결과물이 사라집니다:

```bash
# 빠르게 재설치 (DuckDB는 소스에 캐시됨)
make install-fast
```

## 트러블슈팅

### `pg_config: command not found`

PATH가 설정 안 됨. 터미널에서:
```bash
export PATH="/home/postgres/pgsql-18/bin:/home/postgres/vcpkg:$PATH"
```

### `could not change permissions of directory`

볼륨 권한 문제:
```bash
sudo chown -R postgres:postgres /home/postgres/pgsql-18/data
```

### `caching is currently disabled` 경고

pgduck_server를 캐시 디렉토리와 함께 시작:
```bash
pkill pgduck_server
pgduck_server --cache_dir /tmp/cache &
```

### MinIO 연결 안 됨

컨테이너 내부에서는 `minio:9000`, 호스트에서는 `localhost:9000` 사용.
