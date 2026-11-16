# External extensions to link into libduckdb
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 77392a72077a85897db2519fae11e2c0da972b8a
    INCLUDE_DIR src/include
    APPLY_PATCHES
)

duckdb_extension_load(aws
    GIT_URL https://github.com/duckdb/duckdb-aws
    GIT_TAG 16ae9c135300f750478d648c2496f2952a6f2827
    APPLY_PATCHES
)

duckdb_extension_load(azure
    GIT_URL https://github.com/duckdb/duckdb-azure
    GIT_TAG 906e92458c33309e5d9413e14a85aad7b4d2275f
    APPLY_PATCHES
)

# Extension from this repo
duckdb_extension_load(duckdb_pglake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
