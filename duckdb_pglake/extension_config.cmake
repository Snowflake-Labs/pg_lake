# External extensions to link into libduckdb
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 6b683b1d0b8df3596c65fea97952425ec9469d9a
    INCLUDE_DIR src/include
    ADD_PATCHES
)

# Extension from this repo
duckdb_extension_load(duckdb_pglake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
