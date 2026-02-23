#!/bin/bash
set -e

# pg_lake Installation Script
# Installs pg_lake extensions to PostgreSQL. Can optionally build PostgreSQL from source.

# Determine script directory at the very beginning (before any cd commands)
if command -v realpath &>/dev/null; then
    PG_LAKE_REPO_DIR="$(dirname "$(realpath "${BASH_SOURCE[0]}")")"
elif command -v readlink &>/dev/null; then
    PG_LAKE_REPO_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
else
    PG_LAKE_REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
fi

# Default configuration
PG_VERSION=18
INSTALL_PREFIX="$HOME/pgsql"
SOURCE_DIR="$HOME/pg_lake-deps"
JOBS=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
VCPKG_VERSION=2025.10.17

# Feature flags (defaults optimized for adding pg_lake to existing PostgreSQL)
SKIP_SYSTEM_DEPS=1        # Skip by default - assume deps installed if PostgreSQL exists
BUILD_POSTGRES=0          # Off by default - use --build-postgres to enable
SKIP_VCPKG=0
SKIP_PG_LAKE=0
WITH_TEST_DEPS=0

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_header() {
    echo -e "${BLUE}==>${NC} ${GREEN}$1${NC}"
}

print_info() {
    echo -e "${BLUE}==>${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}Warning:${NC} $1"
}

print_error() {
    echo -e "${RED}Error:${NC} $1" >&2
}

print_skip() {
    echo -e "${YELLOW}Skipping:${NC} $1"
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Install pg_lake extensions to PostgreSQL. By default, installs to an existing
PostgreSQL installation. Use --build-postgres for full development setup.

OPTIONS:
    --build-postgres            Build PostgreSQL from source and initialize database
    --pg-version VERSION        PostgreSQL version to build (16, 17, or 18) [default: 18]
    --prefix DIR                PostgreSQL installation prefix [default: auto-detect or \$HOME/pgsql]
    --source-dir DIR            Directory for cloning source repos [default: \$HOME/pg_lake-deps]
    --jobs N                    Number of parallel build jobs [default: nproc]

    --with-system-deps          Install system build dependencies (auto-enabled with --build-postgres)
    --skip-vcpkg                Skip vcpkg and Azure SDK installation
    --skip-pg-lake              Skip building pg_lake extensions

    --with-test-deps            Install optional test dependencies (PostGIS, pgAudit, pg_cron, azurite)

    -h, --help                  Show this help message

EXAMPLES:
    # Install pg_lake to existing PostgreSQL (most common)
    $0

    # Install with test dependencies
    $0 --with-test-deps

    # Full development environment: build PostgreSQL 18 from source + pg_lake
    $0 --build-postgres --with-test-deps

    # Full development environment with PostgreSQL 17
    $0 --build-postgres --pg-version 17 --with-test-deps

    # Install only pg_lake (assumes vcpkg already set up)
    $0 --skip-system-deps --skip-vcpkg

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --pg-version)
            PG_VERSION="$2"
            shift 2
            ;;
        --prefix)
            INSTALL_PREFIX="$2"
            shift 2
            ;;
        --source-dir)
            SOURCE_DIR="$2"
            shift 2
            ;;
        --jobs)
            JOBS="$2"
            shift 2
            ;;
        --build-postgres)
            BUILD_POSTGRES=1
            SKIP_SYSTEM_DEPS=0  # Auto-enable system deps when building PostgreSQL
            shift
            ;;
        --with-system-deps)
            SKIP_SYSTEM_DEPS=0
            shift
            ;;
        --skip-vcpkg)
            SKIP_VCPKG=1
            shift
            ;;
        --skip-pg-lake)
            SKIP_PG_LAKE=1
            shift
            ;;
        --with-test-deps)
            WITH_TEST_DEPS=1
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate PostgreSQL version
if [[ ! "$PG_VERSION" =~ ^(16|17|18)$ ]]; then
    print_error "Invalid PostgreSQL version: $PG_VERSION. Must be 16, 17, or 18."
    exit 1
fi

# Determine PostgreSQL branch
case $PG_VERSION in
    16) PG_BRANCH="REL_16_STABLE" ;;
    17) PG_BRANCH="REL_17_STABLE" ;;
    18) PG_BRANCH="REL_18_STABLE" ;;
esac

# Determine PostgreSQL installation paths
if [[ $BUILD_POSTGRES -eq 0 ]]; then
    # Not building PostgreSQL - try to detect existing installation
    if command -v pg_config &>/dev/null; then
        PG_INSTALL_DIR=$(pg_config --bindir | sed 's|/bin$||')
        PG_BIN="$(pg_config --bindir)"
        DETECTED_VERSION=$(pg_config --version | sed -n 's/^PostgreSQL \([0-9]\+\).*/\1/p')

        # Warn if detected version doesn't match requested version
        if [[ -n "$DETECTED_VERSION" ]] && [[ "$DETECTED_VERSION" != "$PG_VERSION" ]]; then
            print_warning "Detected PostgreSQL $DETECTED_VERSION, but --pg-version is set to $PG_VERSION"
            print_warning "Using detected version $DETECTED_VERSION"
            PG_VERSION="$DETECTED_VERSION"
        fi

        # Set PGDATA if it exists
        if [[ -n "$PGDATA" ]] && [[ -d "$PGDATA" ]]; then
            : # Keep existing PGDATA
        else
            PGDATA="$PG_INSTALL_DIR/data"
        fi
    else
        print_error "pg_config not found in PATH. Please either:"
        print_error "  1. Install PostgreSQL and ensure pg_config is in PATH, or"
        print_error "  2. Use --build-postgres to build PostgreSQL from source"
        exit 1
    fi
else
    # Building PostgreSQL from source
    PG_INSTALL_DIR="$INSTALL_PREFIX/$PG_VERSION"
    PG_BIN="$PG_INSTALL_DIR/bin"
    PGDATA="$PG_INSTALL_DIR/data"
fi

# Detect OS
detect_os() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
    elif [[ -f /etc/os-release ]]; then
        . /etc/os-release
        if [[ "$ID" == "debian" ]] || [[ "$ID" == "ubuntu" ]] || [[ "$ID_LIKE" == *"debian"* ]]; then
            OS="debian"
        elif [[ "$ID" == "rhel" ]] || [[ "$ID" == "almalinux" ]] || [[ "$ID" == "rocky" ]] || [[ "$ID_LIKE" == *"rhel"* ]]; then
            OS="rhel"
        else
            print_error "Unsupported Linux distribution: $ID"
            exit 1
        fi
    else
        print_error "Unable to detect operating system"
        exit 1
    fi
}

# Install system dependencies
install_system_deps() {
    if [[ $SKIP_SYSTEM_DEPS -eq 1 ]]; then
        print_skip "System dependencies installation"
        return
    fi

    print_header "Installing system build dependencies for $OS"

    case $OS in
        debian)
            sudo apt-get update
            sudo apt-get install -y \
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
                libipc-run-perl \
                jq \
                git \
                pkg-config \
                python3-dev
            ;;
        rhel)
            sudo dnf -y update
            sudo dnf -y install epel-release
            sudo dnf config-manager --enable crb 2>/dev/null || sudo dnf config-manager --set-enabled crb 2>/dev/null || true
            sudo dnf -y install \
                cmake \
                ninja-build \
                readline-devel \
                zlib-devel \
                flex \
                bison \
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
                libcurl-devel \
                patch \
                which \
                gcc-c++ \
                git \
                pkgconfig \
                python3-devel
            ;;
        macos)
            # Check for Xcode command line tools
            if ! xcode-select -p &>/dev/null; then
                print_info "Installing Xcode command line tools..."
                xcode-select --install
                print_warning "Please complete the Xcode installation and re-run this script."
                exit 1
            fi

            brew update
            brew install \
                cmake \
                ninja \
                readline \
                zlib \
                libxml2 \
                libxslt \
                icu4c \
                openssl@3 \
                geos \
                proj \
                gdal \
                json-c \
                protobuf-c \
                lz4 \
                xz \
                snappy \
                jansson \
                curl \
                libtool \
                flex \
                bison \
                diffutils \
                jq \
                ossp-uuid \
                perl \
                pkg-config \
                python@3

            # Install specific cmake version for vcpkg compatibility
            if ! brew list cmake@3.31.1 &>/dev/null; then
                print_info "Installing cmake 3.31.1 for vcpkg compatibility..."
                brew tap-new $USER/local-cmake 2>/dev/null || true
                brew tap homebrew/core --force
                brew extract --version=3.31.1 cmake $USER/local-cmake
                brew install $USER/local-cmake/cmake@3.31.1
            fi
            ;;
    esac

    print_info "System dependencies installed successfully"
}

# Build and install PostgreSQL
install_postgres() {
    if [[ $BUILD_POSTGRES -eq 0 ]]; then
        return  # Not building PostgreSQL
    fi

    print_header "Building PostgreSQL $PG_VERSION from source"

    # Check if already installed
    if [[ -f "$PG_BIN/postgres" ]]; then
        print_info "PostgreSQL $PG_VERSION already installed at $PG_INSTALL_DIR"
        return
    fi

    mkdir -p "$SOURCE_DIR"
    cd "$SOURCE_DIR"

    # Clone if needed
    if [[ ! -d "postgres-$PG_VERSION" ]]; then
        print_info "Cloning PostgreSQL $PG_VERSION..."
        git clone https://github.com/postgres/postgres.git -b "$PG_BRANCH" "postgres-$PG_VERSION"
    else
        print_info "PostgreSQL source already cloned"
    fi

    cd "postgres-$PG_VERSION"

    # Check if IPC::Run is available for TAP tests
    ENABLE_TAP_TESTS=""
    if perl -MIPC::Run -e 1 2>/dev/null; then
        print_info "Perl IPC::Run module available - enabling TAP tests"
        ENABLE_TAP_TESTS="--enable-tap-tests"
    else
        print_warning "Perl IPC::Run module not available - TAP tests will be disabled"
        print_warning "To enable TAP tests, install IPC::Run:"
        print_warning "  NixOS: nix-env -iA nixpkgs.perlPackages.IPCRun"
        print_warning "  Others: cpan IPC::Run"
    fi

    # Configure based on OS
    print_info "Configuring PostgreSQL..."
    if [[ "$OS" == "macos" ]]; then
        # macOS-specific configuration
        export PATH="/opt/homebrew/opt/bison/bin:/opt/homebrew/opt/flex/bin:$PATH"
        export LDFLAGS="-L/opt/homebrew/opt/icu4c/lib -L/opt/homebrew/opt/openssl@3/lib"
        export CPPFLAGS="-I/opt/homebrew/opt/icu4c/include -I/opt/homebrew/opt/openssl@3/include"
        export PKG_CONFIG_PATH="/opt/homebrew/opt/icu4c/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig"

        ./configure \
            --prefix="$PG_INSTALL_DIR" \
            --enable-injection-points \
            $ENABLE_TAP_TESTS \
            --enable-debug \
            --enable-cassert \
            --enable-depend \
            CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer" \
            --with-openssl \
            --with-libxml \
            --with-libxslt \
            --with-icu \
            --with-lz4 \
            --with-python \
            --with-readline \
            --with-includes=/opt/homebrew/include/ \
            --with-libraries=/opt/homebrew/lib \
            PG_SYSROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX.sdk
    else
        # Linux configuration
        ./configure \
            --prefix="$PG_INSTALL_DIR" \
            --enable-injection-points \
            $ENABLE_TAP_TESTS \
            --enable-debug \
            --enable-cassert \
            --enable-depend \
            CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer" \
            --with-openssl \
            --with-libxml \
            --with-libxslt \
            --with-icu \
            --with-lz4 \
            --with-python
    fi

    print_info "Building PostgreSQL with $JOBS parallel jobs..."
    make -j "$JOBS"
    make install

    # Install all contrib modules (required for tests)
    print_info "Installing contrib modules..."
    make -C contrib install

    # Install test modules
    if [[ -d src/test/modules/injection_points ]]; then
        make -C src/test/modules/injection_points install
    fi
    if [[ -d src/test/isolation ]]; then
        make -C src/test/isolation install
    fi

    print_info "PostgreSQL $PG_VERSION installed successfully to $PG_INSTALL_DIR"
}

# Install vcpkg and Azure dependencies
install_vcpkg() {
    if [[ $SKIP_VCPKG -eq 1 ]]; then
        print_skip "vcpkg and Azure SDK installation"
        return
    fi

    print_header "Installing vcpkg and Azure SDK dependencies"

    mkdir -p "$SOURCE_DIR"
    cd "$SOURCE_DIR"

    # Clone vcpkg if needed
    if [[ ! -d "vcpkg" ]]; then
        print_info "Cloning vcpkg $VCPKG_VERSION..."
        git clone --recurse-submodules --branch "$VCPKG_VERSION" https://github.com/Microsoft/vcpkg.git
    else
        print_info "vcpkg already cloned"
    fi

    # Bootstrap vcpkg if needed
    if [[ ! -f "vcpkg/vcpkg" ]]; then
        print_info "Bootstrapping vcpkg..."
        cd vcpkg
        ./bootstrap-vcpkg.sh
        cd ..
    else
        print_info "vcpkg already bootstrapped"
    fi

    # Install packages
    print_info "Installing Azure SDK packages (this may take a while)..."
    ./vcpkg/vcpkg install azure-identity-cpp azure-storage-blobs-cpp azure-storage-files-datalake-cpp openssl

    # Set environment variable
    export VCPKG_TOOLCHAIN_PATH="$SOURCE_DIR/vcpkg/scripts/buildsystems/vcpkg.cmake"

    print_info "vcpkg installed successfully"
}

# Build and install pg_lake
install_pg_lake() {
    if [[ $SKIP_PG_LAKE -eq 1 ]]; then
        print_skip "pg_lake build and installation"
        return
    fi

    print_header "Building pg_lake extensions"

    # Ensure pg_config is in PATH
    export PATH="$PG_BIN:$PATH"

    # Verify pg_config is accessible
    if ! command -v pg_config &>/dev/null; then
        print_error "pg_config not found in PATH. Please ensure PostgreSQL is installed."
        exit 1
    fi

    # Set vcpkg toolchain path
    if [[ -z "$VCPKG_TOOLCHAIN_PATH" ]] && [[ -d "$SOURCE_DIR/vcpkg" ]]; then
        export VCPKG_TOOLCHAIN_PATH="$SOURCE_DIR/vcpkg/scripts/buildsystems/vcpkg.cmake"
    fi

    # Verify we're in the pg_lake repository by checking for key files
    if [[ ! -f "$PG_LAKE_REPO_DIR/Makefile" ]] || [[ ! -d "$PG_LAKE_REPO_DIR/pg_lake_engine" ]]; then
        print_error "install.sh must be run from the pg_lake repository root"
        print_error "Expected location: pg_lake repository containing Makefile and pg_lake_engine/"
        print_error "Detected repository directory: $PG_LAKE_REPO_DIR"
        exit 1
    fi

    print_info "Building pg_lake from repository at: $PG_LAKE_REPO_DIR"
    cd "$PG_LAKE_REPO_DIR"
    make install-fast

    print_info "pg_lake extensions installed successfully"
}

# Initialize PostgreSQL database
init_database() {
    if [[ $BUILD_POSTGRES -eq 0 ]]; then
        return  # Not building/initializing PostgreSQL
    fi

    print_header "Initializing PostgreSQL database cluster"

    export PATH="$PG_BIN:$PATH"

    # Check if already initialized
    if [[ -f "$PGDATA/PG_VERSION" ]]; then
        print_info "Database cluster already initialized at $PGDATA"
        return
    fi

    mkdir -p "$(dirname "$PGDATA")"

    print_info "Running initdb..."
    "$PG_BIN/initdb" -k -D "$PGDATA" --locale=C.UTF-8

    # Add pg_extension_base to shared_preload_libraries
    print_info "Configuring shared_preload_libraries..."
    echo "shared_preload_libraries = 'pg_extension_base'" >> "$PGDATA/postgresql.conf"

    print_info "Database cluster initialized at $PGDATA"
}

# Install test dependencies
install_test_deps() {
    if [[ $WITH_TEST_DEPS -eq 0 ]]; then
        return
    fi

    print_header "Installing test dependencies"

    export PATH="$PG_BIN:$PATH"
    mkdir -p "$SOURCE_DIR"
    cd "$SOURCE_DIR"

    # PostGIS
    # Check if PostGIS is already installed by looking for the extension control file
    if [[ -f "$PG_INSTALL_DIR/share/extension/postgis.control" ]]; then
        print_info "PostGIS already installed"
    else
        print_info "Building PostGIS..."
        if [[ ! -d "postgis" ]]; then
            git clone https://github.com/postgis/postgis.git
        fi
        cd postgis
        ./autogen.sh
        ./configure --prefix="$PG_INSTALL_DIR"
        make -j "$JOBS"
        make install
        cd ..
    fi

    # pgAudit
    if [[ -f "$PG_INSTALL_DIR/share/extension/pgaudit.control" ]]; then
        print_info "pgAudit already installed"
    else
        print_info "Building pgAudit..."
        if [[ ! -d "pgaudit" ]]; then
            git clone https://github.com/pgaudit/pgaudit.git
        fi
        cd pgaudit
        make USE_PGXS=1 install
        cd ..
    fi

    # pg_cron
    if [[ -f "$PG_INSTALL_DIR/share/extension/pg_cron.control" ]]; then
        print_info "pg_cron already installed"
    else
        print_info "Building pg_cron..."
        if [[ ! -d "pg_cron" ]]; then
            git clone https://github.com/citusdata/pg_cron.git
        fi
        cd pg_cron
        make install
        cd ..
    fi

    # Python/pip (needed for pipenv and azurite)
    if ! command -v python3 &>/dev/null || ! command -v pip3 &>/dev/null; then
        print_info "Installing Python 3 and pip..."
        case $OS in
            debian)
                sudo apt-get install -y python3 python3-pip
                ;;
            rhel)
                sudo dnf install -y python3 python3-pip
                ;;
            macos)
                brew install python3
                ;;
        esac
    else
        print_info "Python 3 and pip already installed"
    fi

    # azurite (npm)
    if ! command -v azurite &>/dev/null; then
        print_info "Installing azurite via npm..."
        case $OS in
            debian)
                sudo apt-get install -y nodejs npm
                ;;
            rhel)
                sudo dnf install -y nodejs npm
                ;;
            macos)
                brew install node
                ;;
        esac
        npm install -g azurite
    else
        print_info "azurite already installed"
    fi

    # Python/pipenv
    if ! command -v pipenv &>/dev/null; then
        print_info "Installing pipenv..."
        if command -v pip3 &>/dev/null; then
            pip3 install --user pipenv
        elif command -v python3 &>/dev/null; then
            python3 -m pip install --user pipenv
        else
            print_warning "pip3 or python3 not found. Please install pipenv manually:"
            print_warning "  Debian/Ubuntu: sudo apt-get install pipenv"
            print_warning "  RHEL/AlmaLinux: sudo dnf install pipenv"
            print_warning "  macOS: brew install pipenv"
            print_warning "  Or via pip: python3 -m pip install --user pipenv"
        fi
    else
        print_info "pipenv already installed"
    fi

    # Java 21+ (needed for Spark verification tests and Polaris catalog tests)
    if command -v java &>/dev/null; then
        # Check Java version
        JAVA_VERSION=$(java -version 2>&1 | head -1 | sed -n 's/.*version "\([0-9]*\).*/\1/p')
        if [[ -n "$JAVA_VERSION" ]] && [[ "$JAVA_VERSION" -ge 21 ]]; then
            print_info "Java $JAVA_VERSION already installed"
        else
            print_warning "Java version is $JAVA_VERSION but 21+ is required for tests"
            print_info "Installing Java 21..."
            case $OS in
                debian)
                    sudo apt-get install -y openjdk-21-jdk
                    ;;
                rhel)
                    sudo dnf install -y java-21-openjdk
                    ;;
                macos)
                    brew install openjdk@21
                    # Add to PATH for this session
                    export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"
                    ;;
            esac
        fi
    else
        print_info "Installing Java 21..."
        case $OS in
            debian)
                sudo apt-get install -y openjdk-21-jdk
                ;;
            rhel)
                sudo dnf install -y java-21-openjdk
                ;;
            macos)
                brew install openjdk@21
                export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"
                ;;
        esac
    fi

    # PostgreSQL JDBC driver (needed for Spark verification tests)
    JDBC_DIR="$SOURCE_DIR/jdbc"
    JDBC_VERSION="42.7.10"
    JDBC_JAR="$JDBC_DIR/postgresql-${JDBC_VERSION}.jar"

    if [[ -f "$JDBC_JAR" ]]; then
        print_info "PostgreSQL JDBC driver already downloaded"
    else
        print_info "Downloading PostgreSQL JDBC driver ${JDBC_VERSION}..."
        mkdir -p "$JDBC_DIR"
        if command -v curl &>/dev/null; then
            curl -L -o "$JDBC_JAR" "https://jdbc.postgresql.org/download/postgresql-${JDBC_VERSION}.jar"
        elif command -v wget &>/dev/null; then
            wget -O "$JDBC_JAR" "https://jdbc.postgresql.org/download/postgresql-${JDBC_VERSION}.jar"
        else
            print_error "Neither curl nor wget found. Please install one to download JDBC driver."
            print_error "Or download manually from: https://jdbc.postgresql.org/download/postgresql-${JDBC_VERSION}.jar"
            print_error "And place it at: $JDBC_JAR"
        fi
    fi

    print_info "Test dependencies installed successfully"
}

# Print summary
print_summary() {
    print_header "Setup Complete!"
    echo

    if [[ $BUILD_POSTGRES -eq 1 ]]; then
        # Built PostgreSQL from source
        echo "PostgreSQL $PG_VERSION installed to: $PG_INSTALL_DIR"
        if [[ -f "$PGDATA/PG_VERSION" ]]; then
            echo "Database cluster initialized at: $PGDATA"
        fi
        echo
        echo -e "${GREEN}Next steps:${NC}"
        echo
        echo "1. Add PostgreSQL to your PATH:"
        echo "   export PATH=$PG_BIN:\$PATH"
        echo
        if [[ ! -f "$PGDATA/PG_VERSION" ]]; then
            echo "2. Initialize the database:"
            echo "   initdb -k -D $PGDATA --locale=C.UTF-8"
            echo "   echo \"shared_preload_libraries = 'pg_extension_base'\" >> $PGDATA/postgresql.conf"
            echo
        fi
        echo "2. Start PostgreSQL:"
        echo "   pg_ctl -D $PGDATA -l $PGDATA/logfile start"
        echo
    else
        # Using existing PostgreSQL
        echo "pg_lake extensions installed to PostgreSQL $PG_VERSION at: $PG_INSTALL_DIR"
        echo
        echo -e "${GREEN}Next steps:${NC}"
        echo
        echo "1. Configure PostgreSQL (if not already done):"
        echo "   Add to postgresql.conf: shared_preload_libraries = 'pg_extension_base'"
        echo "   Restart PostgreSQL to load the library"
        echo
    fi

    echo "2. Start pgduck_server (required for pg_lake):"
    echo "   pgduck_server --cache_dir /tmp/pg_lake_cache/"
    echo
    echo "3. Connect and create pg_lake extensions:"
    echo "   psql -c \"CREATE EXTENSION pg_lake CASCADE;\""
    echo "   psql -c \"SET pg_lake_iceberg.default_location_prefix TO 's3://your-bucket/pglake';\""
    echo

    if [[ $WITH_TEST_DEPS -eq 1 ]]; then
        echo "4. Run tests:"
        echo "   cd pg_lake"
        echo "   make check"
        echo
    fi

    echo -e "${BLUE}Documentation:${NC}"
    echo "   Building from source: docs/building-from-source.md"
    echo "   Iceberg tables: docs/iceberg-tables.md"
    echo "   Query data lake files: docs/query-data-lake-files.md"
    echo

    if [[ $BUILD_POSTGRES -eq 1 ]] || [[ -d "$SOURCE_DIR/vcpkg" ]] || [[ $WITH_TEST_DEPS -eq 1 ]] || [[ "$OS" == "macos" ]]; then
        echo -e "${YELLOW}Environment variables to add to your ~/.bashrc or ~/.zshrc:${NC}"
        if [[ $BUILD_POSTGRES -eq 1 ]]; then
            echo "   export PATH=$PG_BIN:\$PATH"
        fi
        if [[ -d "$SOURCE_DIR/vcpkg" ]]; then
            echo "   export VCPKG_TOOLCHAIN_PATH=$SOURCE_DIR/vcpkg/scripts/buildsystems/vcpkg.cmake"
        fi
        if [[ $WITH_TEST_DEPS -eq 1 ]]; then
            JDBC_DIR="$SOURCE_DIR/jdbc"
            JDBC_VERSION="42.7.10"
            JDBC_JAR="$JDBC_DIR/postgresql-${JDBC_VERSION}.jar"
            if [[ -f "$JDBC_JAR" ]]; then
                echo "   export JDBC_DRIVER_PATH=$JDBC_JAR"
            fi
        fi
        if [[ "$OS" == "macos" ]]; then
            echo "   export PATH=\"/opt/homebrew/opt/bison/bin:/opt/homebrew/opt/flex/bin:\$PATH\""
            echo "   export LDFLAGS=\"-L/opt/homebrew/opt/icu4c/lib -L/opt/homebrew/opt/openssl@3/lib\""
            echo "   export CPPFLAGS=\"-I/opt/homebrew/opt/icu4c/include -I/opt/homebrew/opt/openssl@3/include\""
            echo "   export PKG_CONFIG_PATH=\"/opt/homebrew/opt/icu4c/lib/pkgconfig:/opt/homebrew/opt/openssl@3/lib/pkgconfig\""
        fi
    fi
}

# Main execution
main() {
    if [[ $BUILD_POSTGRES -eq 1 ]]; then
        print_header "pg_lake Development Environment Setup (building PostgreSQL from source)"
    else
        print_header "pg_lake Installation (using existing PostgreSQL)"
    fi
    echo

    detect_os
    print_info "Detected OS: $OS"
    print_info "PostgreSQL version: $PG_VERSION"
    print_info "PostgreSQL location: $PG_INSTALL_DIR"
    if [[ $BUILD_POSTGRES -eq 0 ]]; then
        print_info "Mode: Installing to existing PostgreSQL"
    else
        print_info "Mode: Building PostgreSQL from source"
    fi
    print_info "Source directory: $SOURCE_DIR"
    print_info "Build jobs: $JOBS"
    echo

    install_system_deps
    install_postgres
    install_vcpkg
    install_pg_lake
    init_database
    install_test_deps

    echo
    print_summary
}

main
