############## dev-container ##############
ARG BASE_IMAGE_OS="almalinux"
ARG BASE_IMAGE_TAG="9"
FROM ${BASE_IMAGE_OS}:${BASE_IMAGE_TAG} AS dev_base

# need to redefine ARGs in each stage
ARG BASE_IMAGE_OS
ARG BASE_IMAGE_TAG

# Set environment variables for non-interactive installations and PostgreSQL/PostGIS versions
ENV PG_LAKE_REF=main
ARG PG16_VERSION=16.11
ARG PG17_VERSION=17.7
ARG PG18_VERSION=18.1
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

# Download PostgreSQL 16, 17, 18, PostGIS
RUN wget https://ftp.postgresql.org/pub/source/v$PG16_VERSION/postgresql-$PG16_VERSION.tar.gz && \
    wget https://ftp.postgresql.org/pub/source/v$PG17_VERSION/postgresql-$PG17_VERSION.tar.gz && \
    wget https://ftp.postgresql.org/pub/source/v$PG18_VERSION/postgresql-$PG18_VERSION.tar.gz && \
    wget https://download.osgeo.org/postgis/source/postgis-$POSTGIS_VERSION.tar.gz && \
    wget https://github.com/pgaudit/pgaudit/archive/refs/heads/$PGAUDIT16_VERSION.tar.gz && \
    wget https://github.com/pgaudit/pgaudit/archive/refs/heads/$PGAUDIT17_VERSION.tar.gz && \
    wget https://github.com/pgaudit/pgaudit/archive/refs/heads/$PGAUDIT18_VERSION.tar.gz && \
    tar -xzf postgresql-$PG16_VERSION.tar.gz && rm postgresql-$PG16_VERSION.tar.gz && \
    tar -xzf postgresql-$PG17_VERSION.tar.gz && rm postgresql-$PG17_VERSION.tar.gz && \
    tar -xzf postgresql-$PG18_VERSION.tar.gz && rm postgresql-$PG18_VERSION.tar.gz && \
    tar -xzf $PGAUDIT16_VERSION.tar.gz && rm $PGAUDIT16_VERSION.tar.gz && \
    tar -xzf $PGAUDIT17_VERSION.tar.gz && rm $PGAUDIT17_VERSION.tar.gz && \
    tar -xzf $PGAUDIT18_VERSION.tar.gz && rm $PGAUDIT18_VERSION.tar.gz && \
    tar -xzf postgis-$POSTGIS_VERSION.tar.gz && rm postgis-$POSTGIS_VERSION.tar.gz

ARG PG_COMPILE_FLAGS_16="--enable-debug --enable-cassert --enable-depend --with-openssl --with-libxml --with-libxslt --with-icu --with-uuid=ossp --with-lz4"
# PG 17-18 have asserts disabled
ARG PG_COMPILE_FLAGS_17="--enable-debug --enable-depend --with-openssl --with-libxml --with-libxslt --with-icu --with-uuid=ossp --with-lz4 --enable-injection-points"
ARG PG_COMPILE_FLAGS_18="--enable-debug --enable-depend --with-openssl --with-libxml --with-libxslt --with-icu --with-uuid=ossp --with-lz4 --enable-injection-points"
ENV PGBASEDIR=/home/postgres

# Compile and install PostgreSQL 16 with pgindent
RUN cd postgresql-$PG16_VERSION && \
    ./configure --prefix=$PGBASEDIR/pgsql-16 $PG_COMPILE_FLAGS_16 && \
    make -j `nproc` install && \
    make -C src/test/isolation install && \
    make -j `nproc` -C src/tools/pg_bsd_indent install && \
    cp src/tools/pgindent/pgindent $PGBASEDIR/pgsql-16/bin/ && \
    cp src/tools/pgindent/typedefs.list $PGBASEDIR/pgsql-16/share/ && \
    mkdir -pv $PGBASEDIR/pgsql-16/regress && cp -r src/test/regress/* $PGBASEDIR/pgsql-16/regress && \
    (cd contrib && make -j `nproc` install) && \
    cd .. && rm -rf postgresql-$PG16_VERSION*

# Compile and install PostgreSQL 17 with pgindent
RUN cd postgresql-$PG17_VERSION && \
    ./configure --prefix=$PGBASEDIR/pgsql-17 $PG_COMPILE_FLAGS_17 && \
    make -j `nproc` && make install && \
    make -C src/test/isolation install && \
    make -C src/test/modules/injection_points install && \
    make -j `nproc` -C src/tools/pg_bsd_indent install && \
    cp src/tools/pgindent/pgindent $PGBASEDIR/pgsql-17/bin/ && \
    cp src/tools/pgindent/typedefs.list $PGBASEDIR/pgsql-17/share/ && \
    mkdir -pv $PGBASEDIR/pgsql-17/regress && cp -r src/test/regress/* $PGBASEDIR/pgsql-17/regress && \
    (cd contrib && make -j `nproc` install) && \
    cd .. && rm -rf postgresql-$PG17_VERSION*

# Compile and install PostgreSQL 18 with pgindent
RUN cd postgresql-$PG18_VERSION && \
    ./configure --prefix=$PGBASEDIR/pgsql-18 $PG_COMPILE_FLAGS_18 && \
    make -j `nproc` && make install && \
    make -C src/test/isolation install && \
    make -C src/test/modules/injection_points install && \
    make -j `nproc` -C src/tools/pg_bsd_indent install && \
    cp src/tools/pgindent/pgindent $PGBASEDIR/pgsql-18/bin/ && \
    cp src/tools/pgindent/typedefs.list $PGBASEDIR/pgsql-18/share/ && \
    mkdir -pv $PGBASEDIR/pgsql-18/regress && cp -r src/test/regress/* $PGBASEDIR/pgsql-18/regress && \
    (cd contrib && make -j `nproc` install) && \
    cd .. && rm -rf postgresql-$PG18_VERSION*

# Compile and install PG AUDIT 16
RUN cd pgaudit-$PGAUDIT16_VERSION && \
    make -j `nproc` install USE_PGXS=1 PG_CONFIG=$PGBASEDIR/pgsql-16/bin/pg_config

# Compile and install PG AUDIT 17
RUN cd pgaudit-$PGAUDIT17_VERSION && \
    make -j `nproc` install USE_PGXS=1 PG_CONFIG=$PGBASEDIR/pgsql-17/bin/pg_config

# Compile and install PG AUDIT 18
RUN cd pgaudit-$PGAUDIT18_VERSION && \
    make -j `nproc` install USE_PGXS=1 PG_CONFIG=$PGBASEDIR/pgsql-18/bin/pg_config

# Compile and install PostGIS
RUN cd postgis-$POSTGIS_VERSION && \
    ./configure --prefix=$PGBASEDIR/pgsql-16/ --with-pgconfig=$PGBASEDIR/pgsql-16/bin/pg_config && make -j `nproc` && make install && make clean && \
    ./configure --prefix=$PGBASEDIR/pgsql-17/ --with-pgconfig=$PGBASEDIR/pgsql-17/bin/pg_config && make -j `nproc` && make install && make clean && \
    ./configure --prefix=$PGBASEDIR/pgsql-18/ --with-pgconfig=$PGBASEDIR/pgsql-18/bin/pg_config && make -j `nproc` && make install && make clean && \
    cd .. && rm -rf postgis-$POSTGIS_VERSION*

# Compile and install pg_cron
RUN git clone https://github.com/citusdata/pg_cron.git && \
    cd pg_cron && \
    make install PG_CONFIG=$PGBASEDIR/pgsql-16/bin/pg_config && make clean PG_CONFIG=$PGBASEDIR/pgsql-16/bin/pg_config && \
    make install PG_CONFIG=$PGBASEDIR/pgsql-17/bin/pg_config && make clean PG_CONFIG=$PGBASEDIR/pgsql-17/bin/pg_config && \
    make install PG_CONFIG=$PGBASEDIR/pgsql-18/bin/pg_config && make clean PG_CONFIG=$PGBASEDIR/pgsql-18/bin/pg_config && \
    cd .. && rm -rf pg_cron

# Compile and install pg_incremental
RUN git clone https://github.com/CrunchyData/pg_incremental.git && \
    cd pg_incremental && \
    make install PG_CONFIG=$PGBASEDIR/pgsql-16/bin/pg_config && make clean PG_CONFIG=$PGBASEDIR/pgsql-16/bin/pg_config && \
    make install PG_CONFIG=$PGBASEDIR/pgsql-17/bin/pg_config && make clean PG_CONFIG=$PGBASEDIR/pgsql-17/bin/pg_config && \
    make install PG_CONFIG=$PGBASEDIR/pgsql-18/bin/pg_config && make clean PG_CONFIG=$PGBASEDIR/pgsql-18/bin/pg_config && \
    cd .. && rm -rf pg_incremental

# Install vcpkg as postgres user
ARG VCPKG_VERSION=2025.01.13

RUN git clone https://github.com/Microsoft/vcpkg.git -b $VCPKG_VERSION /home/postgres/vcpkg && \
    ./vcpkg/bootstrap-vcpkg.sh && \
    ./vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl

ENV VCPKG_TOOLCHAIN_PATH="/home/postgres/vcpkg/scripts/buildsystems/vcpkg.cmake"

############## base image for pg_lake_postgres and pgduck_server ##############
FROM dev_base AS base

# Clone pg_lake project
RUN git clone https://github.com/snowflake-labs/pg_lake.git \
    --branch ${PG_LAKE_REF} --recurse-submodules /home/postgres/pg_lake

############## pg_lake_builder - Build all pg_lake extensions ##############
FROM base AS pg_lake_builder

# need to redefine ARGs in each stage
ARG PG_MAJOR=18

# Set environment variables for the selected PostgreSQL version
ENV PG_MAJOR=${PG_MAJOR}
ENV PATH=/home/postgres/pgsql-${PG_MAJOR}/bin:$PATH

# Install pg_lake extensions (build happens here, not in final image)
RUN cd pg_lake && \
    make install-pg_lake_spatial && \
    make install-pg_lake_benchmark

############## pgduck_builder - Build duckdb_pglake and pgduck_server ##############
FROM base AS pgduck_builder

# need to redefine ARGs in each stage
ARG PG_MAJOR=18
ARG PGCOMPAT_BUILD_CONFIG=Release

# Set environment variables for the selected PostgreSQL version
ENV PG_MAJOR=${PG_MAJOR}
ENV PATH=/home/postgres/pgsql-${PG_MAJOR}/bin:$PATH

# Install pgduck_server (build happens here, not in final image)
RUN cd pg_lake && \
    make install-pgduck_server && \
    rm -r duckdb_pglake/build

############## runtime_base - Minimal runtime environment ##############
ARG BASE_IMAGE_OS="almalinux"
ARG BASE_IMAGE_TAG="9"
FROM ${BASE_IMAGE_OS}:${BASE_IMAGE_TAG} AS runtime_base

# need to redefine ARGs in each stage
ARG BASE_IMAGE_OS
ARG BASE_IMAGE_TAG

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

############## pg_lake_postgres - Final runtime image (COPY ONLY) ##############
FROM runtime_base AS pg_lake_postgres

# need to redefine ARGs in each stage
ARG PG_MAJOR=18

# Set environment variables for runtime
ENV PG_MAJOR=${PG_MAJOR}
ENV PATH=/home/postgres/pgsql-${PG_MAJOR}/bin:$PATH

# Copy PostgreSQL binaries and libraries for the selected version from dev_base
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR} /home/postgres/pgsql-${PG_MAJOR}

# Copy pg_lake extensions from pg_lake_builder (they're already installed to pgsql-* directories)
# We need to copy the updated lib and share directories with pg_lake extensions
COPY --from=pg_lake_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/lib /home/postgres/pgsql-${PG_MAJOR}/lib
COPY --from=pg_lake_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/share /home/postgres/pgsql-${PG_MAJOR}/share

# Initialize database (lightweight operation, doesn't compile anything)
RUN initdb -D $PGBASEDIR/pgsql-${PG_MAJOR}/data -U postgres --locale=C.UTF-8 --data-checksums

############## pgduck_server - Final runtime image (COPY ONLY) ##############
FROM runtime_base AS pgduck_server

# need to redefine ARGs in each stage
ARG PG_MAJOR=18

# Set environment variables for runtime
ENV PG_MAJOR=${PG_MAJOR}
ENV PATH=/home/postgres/pgsql-${PG_MAJOR}/bin:$PATH

# Copy PostgreSQL binaries and libraries for the selected version
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/bin /home/postgres/pgsql-${PG_MAJOR}/bin
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/lib /home/postgres/pgsql-${PG_MAJOR}/lib
COPY --from=dev_base --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/share /home/postgres/pgsql-${PG_MAJOR}/share

# Copy pgduck_server binaries and libraries from pgduck_builder
# Note: duckdb_pglake installs libduckdb.so (not duckdb_pglake.so)
COPY --from=pgduck_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/bin/ /home/postgres/pgsql-${PG_MAJOR}/bin/
COPY --from=pgduck_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/lib/ /home/postgres/pgsql-${PG_MAJOR}/lib/
COPY --from=pgduck_builder --chown=postgres:postgres /home/postgres/pgsql-${PG_MAJOR}/share/ /home/postgres/pgsql-${PG_MAJOR}/share/
