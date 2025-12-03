.PHONY: all clean format debug release duckdb_debug duckdb_release pull update patch_duckdb

all: release

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

EXT_NAME=duckdb_pglake
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
EXT_FLAGS=-DBUILD_EXTENSIONS="tpch;json;icu"

# GitHub actions uses a hash of the Makefile and source
# files as a cache key. When we update the DuckDB version,
# we should update the line below to trigger a new hash.
DUCKDB_VERSION=1.3.2

# We install libduckdb.so into the postgres direcotry
PG_CONFIG ?= pg_config
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)
PG_INCLUDEDIR := $(shell $(PG_CONFIG) --includedir)

# Remainder is largely taken from https://github.com/duckdb/extension-ci-tools/blob/main/makefiles/duckdb_extension.Makefile
# but with some modifications
TEST_PATH="/test/unittest"
DUCKDB_PATH="/duckdb"

DUCKDB_SRCDIR ?= "./duckdb/"

# For non-MinGW windows the path is slightly different
ifeq ($(OS),Windows_NT)
ifneq ($(CXX),g++)
	TEST_PATH="/test/Release/unittest.exe"
	DUCKDB_PATH="/Release/duckdb.exe"
endif
endif

#### Core extensions, allows easily building one of the core extensions
ifneq ($(CORE_EXTENSIONS),)
	CORE_EXTENSION_VAR:=-DCORE_EXTENSIONS="$(CORE_EXTENSIONS)"
endif

#### OSX config
OSX_BUILD_FLAG=
ifneq (${OSX_BUILD_ARCH}, "")
	OSX_BUILD_FLAG=-DOSX_BUILD_ARCH=${OSX_BUILD_ARCH}
endif

ifeq ("${OSX_BUILD_ARCH}", "arm64")
	RUST_FLAGS=-DRust_CARGO_TARGET=aarch64-apple-darwin
else ifeq ("${OSX_BUILD_ARCH}", "x86_64")
	RUST_FLAGS=-DRust_CARGO_TARGET=x86_64-apple-darwin
endif

#### VCPKG config
VCPKG_TOOLCHAIN_PATH?=
ifneq ("${VCPKG_TOOLCHAIN_PATH}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_MANIFEST_DIR='${PROJ_DIR}' -DVCPKG_BUILD=1 -DCMAKE_TOOLCHAIN_FILE='${VCPKG_TOOLCHAIN_PATH}'
endif
ifneq ("${VCPKG_TARGET_TRIPLET}", "")
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DVCPKG_TARGET_TRIPLET='${VCPKG_TARGET_TRIPLET}'
endif

#### Enable Ninja as generator
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja" -DFORCE_COLORED_OUTPUT=1
endif

#### Configuration for this extension
EXTENSION_FLAGS=-DDUCKDB_EXTENSION_CONFIGS='${EXT_CONFIG}' -DPG_INCLUDEDIR='${PG_INCLUDEDIR}' -DPG_LIBDIR='${PG_LIBDIR}'

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 $(EXTENSION_FLAGS) ${EXT_FLAGS} $(CORE_EXTENSION_VAR) $(OSX_BUILD_FLAG) $(RUST_FLAGS) $(TOOLCHAIN_FLAGS) -DDUCKDB_EXPLICIT_PLATFORM='${DUCKDB_PLATFORM}' -DCUSTOM_LINKER=${CUSTOM_LINKER}

# Detect operating system
UNAME_S := $(shell uname -s)

# Detect number of cores
ifeq ($(UNAME_S),Linux)
	NCORES := $(shell nproc)
else ifeq ($(UNAME_S),Darwin)
	NCORES := $(shell sysctl hw.ncpu | awk '{print $$2}')
else
	NCORES := 4
endif

# Conditionally set the library name
ifeq ($(UNAME_S),Linux)
    LIB_NAME = libduckdb.so
endif
ifeq ($(UNAME_S),Darwin)
    LIB_NAME = libduckdb.dylib
endif

# Extract the PGCOMPAT_BUILD_CONFIG environment variable, default to RelWithDebInfo if not set
# We prefer to build with debug info (-g) as it makes easier to debug production issues
# the downside is that the binary becomes larger, which we think worth it
PGCOMPAT_BUILD_CONFIG ?= RelWithDebInfo

# Determine the DEBUG_INFO_CONFIG_OPTION based on PGCOMPAT_BUILD_CONFIG
ifeq ($(PGCOMPAT_BUILD_CONFIG),Release)
DEBUG_INFO_CONFIG_OPTION = Release
else
DEBUG_INFO_CONFIG_OPTION = RelWithDebInfo
endif

$(LIB_NAME):
	$(MAKE) release

install: $(LIB_NAME)
	install $(LIB_NAME) $(DESTDIR)$(PG_LIBDIR)/$(LIB_NAME)
	install -d $(DESTDIR)$(PG_INCLUDEDIR)/duckdb
	install -d duckdb/src/include/duckdb/* $(DESTDIR)$(PG_INCLUDEDIR)/duckdb
	install duckdb/src/include/duckdb.h $(DESTDIR)$(PG_INCLUDEDIR)
	install duckdb/src/include/duckdb.hpp $(DESTDIR)$(PG_INCLUDEDIR)
	install duckdb/src/include/duckdb_extension.h $(DESTDIR)$(PG_INCLUDEDIR)

uninstall:
	rm -f $(DESTDIR)$(PG_LIBDIR)/$(LIB_NAME)
	rm -rf $(DESTDIR)$(PG_INCLUDEDIR)/duckdb
	rm -f $(DESTDIR)$(PG_INCLUDEDIR)/duckdb.h
	rm -f $(DESTDIR)$(PG_INCLUDEDIR)/duckdb.hpp
	rm -f $(DESTDIR)$(PG_INCLUDEDIR)/duckdb_extension.h

debug: patch_duckdb
	mkdir -p  build/debug
	cmake $(GENERATOR) $(BUILD_FLAGS) $(CLIENT_FLAGS) -DCMAKE_BUILD_TYPE=Debug -S ./duckdb/ -B build/debug
	cmake --build build/debug -j $(NCORES)

release: patch_duckdb
	mkdir -p build/release
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) -DCMAKE_BUILD_TYPE=$(DEBUG_INFO_CONFIG_OPTION) -S $(DUCKDB_SRCDIR) -B build/release
	cmake --build build/release -j $(NCORES)
	cp build/release/src/$(LIB_NAME) .

#### Misc

# patch exit code 1 means the patch is already applied, since we might run this multiple times, be robust
patch_duckdb:
	cd duckdb && \
	find ../patches/duckdb -type f -name '*.patch' -print | xargs -0 printf '%s ' | \
	while read -r patch; do \
	patch -l -p1 -N < "$$patch" || [ $$? -eq 1 ]; \
	done
	cd duckdb-postgres && \
	find ../patches/duckdb-postgres -type f -name '*.patch' -print | xargs -0 printf '%s ' | \
	while read -r patch; do \
	patch -l -p1 -N < "$$patch" || [ $$? -eq 1 ]; \
	done
	cd duckdb-azure && \
	find ../patches/duckdb-azure -type f -name '*.patch' -print | xargs -0 printf '%s ' | \
	while read -r patch; do \
	patch -l -p1 -N < "$$patch" || [ $$? -eq 1 ]; \
	done

format:
	find src/ -iname *.hpp -o -iname *.cpp | xargs clang-format --sort-includes=0 -style=file -i
	cmake-format -i CMakeLists.txt
update:
	git submodule update --remote --merge
pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf testext
	rm -f libduckdb.so
