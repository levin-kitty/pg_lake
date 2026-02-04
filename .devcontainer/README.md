# Devcontainer 주의 사항

## 1) 리소스 요구사항
- 빌드가 무겁습니다. **Docker 메모리 16GB 이상**을 권장합니다.
- 첫 빌드는 PostgreSQL 17 + PostGIS + pgaudit + pg_cron + vcpkg 등을 모두 컴파일하므로 시간이 오래 걸립니다.

## 2) 보안/접근 설정 (개발 전용)
- `pg_hba.conf`는 **모든 IP를 trust**로 열어둡니다.
- `listen_addresses = '*'`로 외부 접속을 허용합니다.
- **로컬 개발 전용 설정**입니다. 프로덕션에서 사용하면 안 됩니다.

## 3) MinIO 기본 설정
- 기본 계정: `testkey` / `testpassword`
- 기본 버킷: `localbucket`
- 기본 prefix: `s3://localbucket/pg_lake/`
- 변경하려면 `.devcontainer/docker-compose.yml`의 환경 변수를 수정하세요.

## 4) 테스트 환경의 S3는 MinIO가 아님
- pytest는 **Moto(Mock S3)**를 사용합니다.
- 테스트용 값:
  - Access/Secret: `testing` / `testing`
  - Bucket: `testbucketcdw`
  - Endpoint: `http://localhost:5999`
- MinIO와는 별개입니다.

## 5) pgduck_server는 소스 빌드 후 사용 가능
- devcontainer 시작 시 `pgduck_server`가 **설치되어 있지 않으면 실행되지 않습니다**.
- 아래 중 하나를 실행해야 합니다:
  - `make install-fast`
  - `make install-pgduck_server`

## 6) Polaris(REST Catalog) 테스트 주의
- Polaris 빌드는 **Java 21**이 필요합니다.
- JAR가 없다면 아래를 실행해 설치합니다:
  - `make install-rest_catalog`
- Dockerfile을 바꾼 경우(예: Java 버전 변경) **devcontainer 재빌드**가 필요합니다.

## 7) 데이터 보존/초기화
- PostgreSQL 데이터는 docker volume(`pgdata`)에 유지됩니다.
- 초기화가 필요하면 **볼륨 삭제** 후 재시작하세요.

## 8) 하위 모듈(submodule)
- `postCreateCommand`에서 submodule을 자동으로 받습니다.
- 네트워크가 불안하면 submodule 체크아웃이 실패할 수 있습니다.

## 9) Dockerfile 변경 시
- `.devcontainer/Dockerfile`을 수정하면 **이미지 재빌드**가 필요합니다.

## 10) 체크 실행 전 환경변수/명령 요약
- 기본 환경변수는 `/etc/profile.d/pg_lake_env.sh`에 들어있습니다.
  - `PG_CONFIG`, `PG_REGRESS_DIR`, `VCPKG_TOOLCHAIN_PATH`, `LD_LIBRARY_PATH` 등
- pgduck_server를 **띄우지 않고** 테스트하려면:
  - `SKIP_PGDUCK_SERVER=1 bash .devcontainer/scripts/start-services.sh`
- 개별 pytest 실행 시 공통 모듈 경로:
  - `PYTHONPATH=./test_common pipenv run pytest -q <test>`
- 일반 로컬 체크 순서(요약):
  1) `pipenv install --dev`
  2) `make install-fast`
  3) `SKIP_PGDUCK_SERVER=1 bash .devcontainer/scripts/start-services.sh`
  4) `make check-local`

## 11) DuckDB 빌드 OOM 회피
- `duckdb_pglake` 링크 단계는 메모리 스파이크가 커서 `ld`가 강제 종료될 수 있습니다.
- 아래처럼 병렬도를 낮추면 대부분 해결됩니다:
  - `make install-fast NCORES=2`
  - 더 안정적으로는 `make install-fast NCORES=1`
  - 빠르면서 메모리 절감: `make install-fast NCORES=2 PGCOMPAT_BUILD_CONFIG=Release`

## 12) 최종 검증(Polaris 포함)
- Java 버전 확인:
  - `java -version` → **21**이어야 Polaris 빌드 성공
- Polaris JAR 설치(1회):
  - `make install-rest_catalog`
- REST 카탈로그 관련 테스트:
  - `make check-pg_lake_table`
  - `make check-pg_lake_iceberg`
  - (시간이 부담되면 pytest로 `-k "polaris"`만 선별 실행)
