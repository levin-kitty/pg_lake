# syntax=docker/dockerfile:1
ARG BASE_IMAGE_OS=almalinux
ARG BASE_IMAGE_TAG=9
FROM ${BASE_IMAGE_OS}:${BASE_IMAGE_TAG}

ARG PG17_VERSION=17.7
ARG POSTGIS_VERSION=3.6.0
ARG PGAUDIT17_VERSION=REL_17_STABLE
ARG VCPKG_VERSION=2025.01.13

ENV PG_MAJOR=17
ENV PGBASEDIR=/home/postgres
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk
ENV PATH=/home/postgres/pgsql-17/bin:${JAVA_HOME}/bin:$PATH
ENV LD_LIBRARY_PATH=/home/postgres/pgsql-17/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}

# Build and runtime dependencies
RUN dnf -y update && \
    dnf -y install epel-release dnf-plugins-core && \
    dnf config-manager --enable crb && \
    dnf -y install \
      ca-certificates \
      cmake \
      ninja-build \
      wget \
      git \
      readline-devel \
      zlib-devel \
      flex \
      bison \
      sudo \
      nano \
      libxml2-devel \
      libxslt-devel \
      libicu-devel \
      openssl-devel \
      geos-devel \
      proj-devel \
      gdal-devel \
      json-c-devel \
      protobuf-c-devel \
      uuid-devel \
      lz4-devel \
      xz-devel \
      snappy-devel \
      perl \
      perl-IPC-Run \
      perl-IPC-Cmd \
      libtool \
      jansson-devel \
      jq \
      diffutils \
      libcurl-devel \
      patch \
      procps-ng \
      which \
      gcc-c++ \
      nodejs \
      python3.11 \
      python3-pip \
      java-21-openjdk-devel \
      postgresql-jdbc.noarch \
      unzip \
      zip && \
    "$JAVA_HOME/bin/java" -version && \
    "$JAVA_HOME/bin/javac" -version && \
    dnf clean all

# Python tooling
RUN python3.11 -m ensurepip --upgrade && \
    python3.11 -m pip install --upgrade pip && \
    python3.11 -m pip install pipenv && \
    python3.11 -m pip install pre-commit==4.3.0

# Azure emulator (azurite) for tests
RUN npm install -g azurite

# Create postgres user
RUN useradd -u 1001 -m -s /bin/bash postgres && \
    echo "postgres ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/postgres

# Shell environment defaults
RUN cat > /etc/profile.d/pg_lake_env.sh <<'EOT'
export PG_MAJOR=17
export PGBASEDIR=/home/postgres
export PATH=/home/postgres/pgsql-17/bin:$PATH
export PG_CONFIG=/home/postgres/pgsql-17/bin/pg_config
export PG_REGRESS_DIR=/home/postgres/pgsql-17/regress
export VCPKG_TOOLCHAIN_PATH=/home/postgres/vcpkg/scripts/buildsystems/vcpkg.cmake
export LD_LIBRARY_PATH=/home/postgres/pgsql-17/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH
if [ -z "${JDBC_DRIVER_PATH:-}" ]; then
  for p in /usr/share/java/postgresql-jdbc.jar /usr/share/java/postgresql.jar /usr/share/java/postgresql-jdbc4.jar; do
    if [ -f "$p" ]; then
      export JDBC_DRIVER_PATH="$p"
      break
    fi
  done
fi
EOT

USER 1001:1001
WORKDIR /home/postgres

# Download PostgreSQL 17, PostGIS, and pgaudit
RUN wget https://ftp.postgresql.org/pub/source/v${PG17_VERSION}/postgresql-${PG17_VERSION}.tar.gz && \
    wget https://download.osgeo.org/postgis/source/postgis-${POSTGIS_VERSION}.tar.gz && \
    wget https://github.com/pgaudit/pgaudit/archive/refs/heads/${PGAUDIT17_VERSION}.tar.gz && \
    tar -xzf postgresql-${PG17_VERSION}.tar.gz && rm postgresql-${PG17_VERSION}.tar.gz && \
    tar -xzf postgis-${POSTGIS_VERSION}.tar.gz && rm postgis-${POSTGIS_VERSION}.tar.gz && \
    tar -xzf ${PGAUDIT17_VERSION}.tar.gz && rm ${PGAUDIT17_VERSION}.tar.gz

ARG PG_COMPILE_FLAGS_17="--enable-debug --enable-depend --with-openssl --with-libxml --with-libxslt --with-icu --with-uuid=ossp --with-lz4 --enable-injection-points"

# Build PostgreSQL 17 with pgindent and test tools
RUN cd postgresql-${PG17_VERSION} && \
    ./configure --prefix=${PGBASEDIR}/pgsql-17 ${PG_COMPILE_FLAGS_17} && \
    make -j "$(nproc)" && make install && \
    make -C src/test/isolation install && \
    make -C src/test/modules/injection_points install && \
    make -j "$(nproc)" -C src/tools/pg_bsd_indent install && \
    cp src/tools/pgindent/pgindent ${PGBASEDIR}/pgsql-17/bin/ && \
    cp src/tools/pgindent/typedefs.list ${PGBASEDIR}/pgsql-17/share/ && \
    mkdir -pv ${PGBASEDIR}/pgsql-17/regress && cp -r src/test/regress/* ${PGBASEDIR}/pgsql-17/regress && \
    (cd contrib && make -j "$(nproc)" install) && \
    cd .. && rm -rf postgresql-${PG17_VERSION}*

# Build pgaudit
RUN cd pgaudit-${PGAUDIT17_VERSION} && \
    make -j "$(nproc)" install USE_PGXS=1 PG_CONFIG=${PGBASEDIR}/pgsql-17/bin/pg_config && \
    cd .. && rm -rf pgaudit-${PGAUDIT17_VERSION}

# Build PostGIS
RUN cd postgis-${POSTGIS_VERSION} && \
    ./configure --prefix=${PGBASEDIR}/pgsql-17/ --with-pgconfig=${PGBASEDIR}/pgsql-17/bin/pg_config && \
    make -j "$(nproc)" && make install && make clean && \
    cd .. && rm -rf postgis-${POSTGIS_VERSION}*

# Build pg_cron
RUN git clone https://github.com/citusdata/pg_cron.git && \
    cd pg_cron && \
    make install PG_CONFIG=${PGBASEDIR}/pgsql-17/bin/pg_config && \
    cd .. && rm -rf pg_cron

# Build pg_incremental
RUN git clone https://github.com/CrunchyData/pg_incremental.git && \
    cd pg_incremental && \
    make install PG_CONFIG=${PGBASEDIR}/pgsql-17/bin/pg_config && \
    cd .. && rm -rf pg_incremental

# Install vcpkg for DuckDB extensions
RUN git clone https://github.com/Microsoft/vcpkg.git -b ${VCPKG_VERSION} /home/postgres/vcpkg && \
    /home/postgres/vcpkg/bootstrap-vcpkg.sh && \
    /home/postgres/vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl

ENV VCPKG_TOOLCHAIN_PATH=/home/postgres/vcpkg/scripts/buildsystems/vcpkg.cmake

WORKDIR /workspaces/pg_lake
