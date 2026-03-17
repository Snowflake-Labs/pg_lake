cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME duckdb_pglake)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

find_package(OpenSSL 3 REQUIRED)
find_library(PG_LIBDIR pq REQUIRED)
find_path(PG_INCLUDEDIR libpq-fe.h REQUIRED)

find_package(azure-identity-cpp CONFIG)
find_package(azure-storage-blobs-cpp CONFIG)
find_package(azure-storage-files-datalake-cpp CONFIG)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Optimize Github Actions builds by avoiding vcpkg rebuild
set(VCPKG_DISABLE_COMPILER_TRACKING on)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

project(${TARGET_NAME})
include_directories(src/include
                    duckdb-postgres/src/include
                    duckdb-azure/src/include
                    duckdb/third_party/httplib
                    ${PG_INCLUDEDIR}
                    ${OPENSSL_INCLUDE_DIR}
                    ${DUCKDB_EXTENSION_HTTPFS_INCLUDE_PATH})

file(GLOB PGCOMPAT_SOURCES 
     "src/*.cpp"
     "src/fs/*.cpp"
)

file(GLOB POSTGRES_SOURCES
     "duckdb-postgres/src/*.cpp"
     "duckdb-postgres/src/storage/*.cpp"
)

file(GLOB AZURE_SOURCES
     "duckdb-azure/src/*.cpp"
     "duckdb-azure/src/storage/*.cpp"
)


set(EXTENSION_SOURCES
    ${PGCOMPAT_SOURCES}
    ${POSTGRES_SOURCES}
    ${AZURE_SOURCES}
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})

if(APPLE)
  set(LIBPQ_NAME "libpq.dylib")
else()
  set(LIBPQ_NAME "libpq.so")
endif()

target_link_libraries(${EXTENSION_NAME}
                      ${PG_LIBDIR}/${LIBPQ_NAME}
                      Azure::azure-identity Azure::azure-storage-blobs Azure::azure-storage-files-datalake)
target_include_directories(
  ${EXTENSION_NAME} PRIVATE Azure::azure-identity Azure::azure-storage-blobs
                            Azure::azure-storage-files-datalake)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
