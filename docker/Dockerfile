############## dev-container ##############
ARG BASE_IMAGE_OS="almalinux"
ARG BASE_IMAGE_TAG="9"
FROM ${BASE_IMAGE_OS}:${BASE_IMAGE_TAG} AS dev_base

# need to redefine ARGs in each stage
ARG BASE_IMAGE_OS
ARG BASE_IMAGE_TAG

# Set environment variables for non-interactive installations and PostgreSQL/PostGIS versions
ENV PG_LAKE_REF=main
ENV PG_MAJOR=18
ARG PG16_VERSION=16.10
ARG PG17_VERSION=17.6
ARG PG18_VERSION=18.0
ARG POSTGIS_VERSION=3.6.0
ARG PGAUDIT16_VERSION=REL_16_STABLE
ARG PGAUDIT17_VERSION=REL_17_STABLE
ARG PGAUDIT18_VERSION=REL_18_STABLE

# Install build dependencies
RUN if [ "$BASE_IMAGE_OS" = "almalinux" ]; then \
    dnf -y update && \
    dnf -y install epel-release && \
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
    which \
    gcc-c++ \
    nodejs \
    python3.11 \
    pip \
    java-21-openjdk-devel \
    postgresql-jdbc.noarch \
    unzip \
    zip && \
    alternatives --auto java && alternatives --auto javac && java -version && \
    dnf clean all; \
    fi
RUN if [ "$BASE_IMAGE_OS" = "debian" ]; then \
    apt-get update \
    && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    libxml2-dev \
    libxslt1-dev \
    libicu-dev \
    libssl-dev \
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    libjson-c-dev \
    libprotobuf-c-dev \
    protobuf-c-compiler \
    diffutils \
    uuid-dev \
    libossp-uuid-dev \
    liblz4-dev \
    liblzma-dev \
    libsnappy-dev \
    perl \
    libtool \
    libjansson-dev \
    libcurl4-openssl-dev \
    curl \
    patch \
    g++ \
    unzip \
    zip \
    python3.11 \
    pip \
    npm \
    nodejs \
    libipc-run-perl \
    wget \
    git \
    jq \
    sudo \
    libpostgresql-jdbc-java \
    && apt-get clean && rm -rf /var/lib/apt/lists/*; \
    fi

# Install jdk21 for debian
RUN if [ "$BASE_IMAGE_OS" = "debian" ]; then \
    if [ "$(dpkg --print-architecture)" = "arm64" ]; then \
    ARCH=aarch64; \
    else \
    ARCH=x64; \
    fi && \
    wget https://download.oracle.com/java/21/latest/jdk-21_linux-${ARCH}_bin.tar.gz && \
    mkdir -p /usr/lib/jvm && \
    tar -xzf jdk-21_linux-${ARCH}_bin.tar.gz -C /usr/lib/jvm/ && \
    mv /usr/lib/jvm/jdk-21.0.9 /usr/lib/jvm/java-21-openjdk && \
    rm jdk-21_linux-${ARCH}_bin.tar.gz; \
    fi

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

# Install pipenv and pre-commit
RUN if [ "$BASE_IMAGE_OS" = "almalinux" ]; then \
    python3.11 -m ensurepip --upgrade && \
    python3.11 -m pip install --upgrade pip && \
    python3.11 -m pip install pipenv && \
    python3.11 -m pip install pre-commit="=4.3.0"; \
    fi
RUN if [ "$BASE_IMAGE_OS" = "debian" ]; then \
    python3.11 -m pip install --upgrade pip --break-system-packages && \
    python3.11 -m pip install pipenv --break-system-packages && \
    python3.11 -m pip install pre-commit="=4.3.0" --break-system-packages; \
    fi

# Install azurite
RUN npm install -g azurite

# Create the postgres user with UID 1001 (needed for permission to checkout in GitHub Actions)
RUN useradd -u 1001 -m -s /bin/bash postgres
RUN echo "postgres ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers.d/postgres
USER 1001:1001
WORKDIR /home/postgres

# Set PostgreSQL version variables based on PG_MAJOR
RUN if [ "$PG_MAJOR" = "16" ]; then \
    echo "export PG_VERSION=$PG16_VERSION" >> ~/.pgenv; \
    echo "export PGAUDIT_VERSION=$PGAUDIT16_VERSION" >> ~/.pgenv; \
    echo "export PG_COMPILE_FLAGS='--enable-debug --enable-cassert --enable-depend --with-openssl --with-libxml --with-libxslt --with-icu --with-uuid=ossp --with-lz4'" >> ~/.pgenv; \
    elif [ "$PG_MAJOR" = "17" ]; then \
    echo "export PG_VERSION=$PG17_VERSION" >> ~/.pgenv; \
    echo "export PGAUDIT_VERSION=$PGAUDIT17_VERSION" >> ~/.pgenv; \
    echo "export PG_COMPILE_FLAGS='--enable-debug --enable-depend --with-openssl --with-libxml --with-libxslt --with-icu --with-uuid=ossp --with-lz4 --enable-injection-points'" >> ~/.pgenv; \
    elif [ "$PG_MAJOR" = "18" ]; then \
    echo "export PG_VERSION=$PG18_VERSION" >> ~/.pgenv; \
    echo "export PGAUDIT_VERSION=$PGAUDIT18_VERSION" >> ~/.pgenv; \
    echo "export PG_COMPILE_FLAGS='--enable-debug --enable-depend --with-openssl --with-libxml --with-libxslt --with-icu --with-uuid=ossp --with-lz4 --enable-injection-points'" >> ~/.pgenv; \
    fi && \
    . ~/.pgenv && \
    wget https://ftp.postgresql.org/pub/source/v$PG_VERSION/postgresql-$PG_VERSION.tar.gz && \
    wget https://download.osgeo.org/postgis/source/postgis-$POSTGIS_VERSION.tar.gz && \
    wget https://github.com/pgaudit/pgaudit/archive/refs/heads/$PGAUDIT_VERSION.tar.gz && \
    tar -xzf postgresql-$PG_VERSION.tar.gz && rm postgresql-$PG_VERSION.tar.gz && \
    tar -xzf $PGAUDIT_VERSION.tar.gz && rm $PGAUDIT_VERSION.tar.gz && \
    tar -xzf postgis-$POSTGIS_VERSION.tar.gz && rm postgis-$POSTGIS_VERSION.tar.gz

ENV PGBASEDIR=/home/postgres

# Compile and install PostgreSQL for the selected major version
RUN . ~/.pgenv && \
    cd postgresql-$PG_VERSION && \
    ./configure --prefix=$PGBASEDIR/pgsql-$PG_MAJOR $PG_COMPILE_FLAGS && \
    make -j `nproc` install && \
    make -C src/test/isolation install && \
    (if [ "$PG_MAJOR" != "16" ]; then make -C src/test/modules/injection_points install; fi) && \
    make -j `nproc` -C src/tools/pg_bsd_indent install && \
    cp src/tools/pgindent/pgindent $PGBASEDIR/pgsql-$PG_MAJOR/bin/ && \
    cp src/tools/pgindent/typedefs.list $PGBASEDIR/pgsql-$PG_MAJOR/share/ && \
    mkdir -pv $PGBASEDIR/pgsql-$PG_MAJOR/regress && cp -r src/test/regress/* $PGBASEDIR/pgsql-$PG_MAJOR/regress && \
    (cd contrib && make -j `nproc` install) && \
    cd .. && rm -rf postgresql-$PG_VERSION*

# Compile and install PG AUDIT for the selected major version
RUN . ~/.pgenv && \
    cd pgaudit-$PGAUDIT_VERSION && \
    make -j `nproc` install USE_PGXS=1 PG_CONFIG=$PGBASEDIR/pgsql-$PG_MAJOR/bin/pg_config && \
    cd .. && rm -rf pgaudit-$PGAUDIT_VERSION*

# Compile and install PostGIS for the selected major version
RUN cd postgis-$POSTGIS_VERSION && \
    ./configure --prefix=$PGBASEDIR/pgsql-$PG_MAJOR/ --with-pgconfig=$PGBASEDIR/pgsql-$PG_MAJOR/bin/pg_config && \
    make -j `nproc` && make install && \
    cd .. && rm -rf postgis-$POSTGIS_VERSION*

# Compile and install pg_cron for the selected major version
RUN git clone https://github.com/citusdata/pg_cron.git && \
    cd pg_cron && \
    make install PG_CONFIG=$PGBASEDIR/pgsql-$PG_MAJOR/bin/pg_config && \
    cd .. && rm -rf pg_cron

ENV PATH=/home/postgres/pgsql-$PG_MAJOR/bin:$PATH

# Install vcpkg as postgres user
ARG VCPKG_VERSION=2025.01.13

# Set environment variables to help with download issues
ENV VCPKG_FORCE_SYSTEM_BINARIES=1
ENV VCPKG_MAX_CONCURRENCY=1

RUN git clone https://github.com/Microsoft/vcpkg.git -b $VCPKG_VERSION /home/postgres/vcpkg && \
    ./vcpkg/bootstrap-vcpkg.sh && \
    # Install packages with retry logic for network issues
    (./vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl || \
    (echo "Retrying vcpkg install after network error..." && sleep 5 && \
    ./vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl)) && \
    rm -rf /home/postgres/vcpkg/buildtrees /home/postgres/vcpkg/downloads

ENV VCPKG_TOOLCHAIN_PATH="/home/postgres/vcpkg/scripts/buildsystems/vcpkg.cmake"

############## base image for pg_lake_postgres and pgduck_server ##############
FROM dev_base AS base

# Clone pg_lake project
RUN git clone https://github.com/snowflake-labs/pg_lake.git \
    --branch ${PG_LAKE_REF} --recurse-submodules /home/postgres/pg_lake

############## pg_lake_builder - Build all pg_lake extensions ##############
FROM base AS pg_lake_builder

# Install pg_lake extensions (build happens here, not in final image)
RUN cd pg_lake && \
    make install-pg_extension_base && \
    make install-pg_map && \
    make install-pg_extension_updater && \
    make install-pg_lake_engine && \
    make install-avro && \
    make install-pg_lake_iceberg && \
    make install-pg_lake_table && \
    make install-pg_lake_spatial && \
    make install-pg_lake_copy && \
    make install-pg_lake && \
    make install-pg_lake_benchmark

############## pgduck_builder - Build duckdb_pglake and pgduck_server ##############
FROM base AS pgduck_builder

ARG PGCOMPAT_BUILD_CONFIG=Release

# Install pgduck_server (build happens here, not in final image)
RUN cd pg_lake/duckdb_pglake && make && make install && rm -r build
RUN cd pg_lake/pgduck_server && make && make install

############## runtime_base - Minimal runtime environment ##############
ARG BASE_IMAGE_OS="almalinux"
ARG BASE_IMAGE_TAG="9"
FROM ${BASE_IMAGE_OS}:${BASE_IMAGE_TAG} AS runtime_base

# need to redefine ARGs in each stage
ARG BASE_IMAGE_OS
ARG BASE_IMAGE_TAG
ARG PG_MAJOR=18

# Install ONLY runtime libraries (no -devel packages, no build tools)
RUN if [ "$BASE_IMAGE_OS" = "almalinux" ]; then \
    dnf -y update && \
    dnf -y install epel-release && \
    dnf config-manager --enable crb && \
    dnf -y install --allowerasing \
    ca-certificates \
    readline \
    zlib \
    sudo \
    nano \
    libxml2 \
    libxslt \
    libicu \
    openssl \
    geos \
    proj \
    gdal \
    json-c \
    protobuf-c \
    uuid \
    lz4 \
    xz \
    snappy \
    perl \
    jansson \
    libcurl && \
    dnf clean all; \
    fi

RUN if [ "$BASE_IMAGE_OS" = "debian" ]; then \
    apt-get update \
    && apt-get install -y \
    libreadline8 \
    zlib1g \
    libxml2 \
    libxslt1.1 \
    libicu72 \
    libssl3 \
    libgeos-c1v5 \
    libproj25 \
    libgdal32 \
    libjson-c5 \
    libprotobuf-c1 \
    uuid-runtime \
    libossp-uuid16 \
    liblz4-1 \
    liblzma5 \
    libsnappy1v5 \
    perl \
    libjansson4 \
    libcurl4 \
    curl \
    sudo \
    && apt-get clean && rm -rf /var/lib/apt/lists/*; \
    fi

# Create the postgres user with UID 1001
RUN useradd -u 1001 -m -s /bin/bash postgres
RUN echo "postgres ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers.d/postgres
USER 1001:1001
WORKDIR /home/postgres

ENV PGBASEDIR=/home/postgres
ENV PATH=/home/postgres/pgsql-$PG_MAJOR/bin:$PATH

############## pg_lake_postgres - Final runtime image (COPY ONLY) ##############
FROM runtime_base AS pg_lake_postgres

ARG PG_MAJOR=18

# Copy PostgreSQL binaries and libraries for the selected version from dev_base
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR} /home/postgres/pgsql-${PG_MAJOR}

# Copy pg_lake extensions from pg_lake_builder (they're already installed to pgsql-* directories)
# We need to copy the updated lib and share directories with pg_lake extensions
COPY --from=pg_lake_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/lib /home/postgres/pgsql-${PG_MAJOR}/lib
COPY --from=pg_lake_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/share /home/postgres/pgsql-${PG_MAJOR}/share

# Copy regress test files (needed for some tests)
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/regress /home/postgres/pgsql-${PG_MAJOR}/regress

# Initialize database (lightweight operation, doesn't compile anything)
RUN initdb -D $PGBASEDIR/pgsql-$PG_MAJOR/data -U postgres --locale=C.UTF-8 --data-checksums

############## pgduck_server - Final runtime image (COPY ONLY) ##############
FROM runtime_base AS pgduck_server

ARG PG_MAJOR=18

# Copy PostgreSQL binaries and libraries for the selected version
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/bin/psql /home/postgres/pgsql-${PG_MAJOR}/bin/psql
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/bin/pg_config /home/postgres/pgsql-${PG_MAJOR}/bin/pg_config
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/bin/initdb /home/postgres/pgsql-${PG_MAJOR}/bin/initdb
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/lib /home/postgres/pgsql-${PG_MAJOR}/lib
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/share /home/postgres/pgsql-${PG_MAJOR}/share

# Copy pgduck_server binaries and libraries from pgduck_builder
# Note: duckdb_pglake installs libduckdb.so (not duckdb_pglake.so)
COPY --from=pgduck_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/bin/pgduck_server /home/postgres/pgsql-${PG_MAJOR}/bin/pgduck_server
COPY --from=pgduck_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/lib/ /home/postgres/pgsql-${PG_MAJOR}/lib/
COPY --from=pgduck_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/share/ /home/postgres/pgsql-${PG_MAJOR}/share/
