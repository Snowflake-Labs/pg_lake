# External extensions to link into libduckdb
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 13f8a814d41a978c3f19eb1dc76069489652ea6f
    INCLUDE_DIR src/include
    ADD_PATCHES
)

duckdb_extension_load(aws
    GIT_URL https://github.com/duckdb/duckdb-aws
    GIT_TAG bc15d211f282d1d78fc0d9fda3d09957ba776423
)

duckdb_extension_load(azure
    GIT_URL https://github.com/duckdb/duckdb-azure
    GIT_TAG 7e1ac3333d946a6bf5b4552722743e03f30a47cd
)

# Extension from this repo
duckdb_extension_load(duckdb_pglake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
