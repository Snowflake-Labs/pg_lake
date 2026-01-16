# External extensions to link into libduckdb
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 9c7d34977b10346d0b4cbbde5df807d1dab0b2bf
    INCLUDE_DIR src/include
    ADD_PATCHES
)

duckdb_extension_load(aws
    GIT_URL https://github.com/duckdb/duckdb-aws
    GIT_TAG 55bf3621fb7db254b473c94ce6360643ca38fac0
)

# Extension from this repo
duckdb_extension_load(duckdb_pglake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
