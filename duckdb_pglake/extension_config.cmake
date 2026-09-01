# External extensions to link into libduckdb
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 827222fb45a043a7a852d1f7aae46901492a3cda
    INCLUDE_DIR src/include
)

# Not bumped to duckdb 1.5.5's pin (efa54a99): its RDS secret code returns a
# unique_ptr<KeyValueSecret> as unique_ptr<BaseSecret>, which only compiles
# with C++20 implicit move (P1825). gcc 11 (AlmaLinux 9) rejects it at the
# C++11/14 standard duckdb builds extensions with.
duckdb_extension_load(aws
    GIT_URL https://github.com/duckdb/duckdb-aws
    GIT_TAG b2649e68341a9ee717588dd23f277904727ce793
)

duckdb_extension_load(azure
    GIT_URL https://github.com/duckdb/duckdb-azure
    GIT_TAG 003214c96d0caa39d5c3e27a9e1976a0692c7d37
)

# Extension from this repo
duckdb_extension_load(duckdb_pglake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
