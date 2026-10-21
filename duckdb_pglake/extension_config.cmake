# External extensions to link into libduckdb
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG abdd053708ff3ef9359a5b3da680b128d95e4f84
    INCLUDE_DIR src/include
    APPLY_PATCHES
)

duckdb_extension_load(aws
    GIT_URL https://github.com/duckdb/duckdb-aws
    GIT_TAG de46483edd4aab903a481db5332cbfa7970568f1
    APPLY_PATCHES
)

duckdb_extension_load(azure
    GIT_URL https://github.com/duckdb/duckdb-azure
    GIT_TAG 9abac98725f180936316ae1a3cd8e01ee85b6b61
    APPLY_PATCHES
)

# Extension from this repo
duckdb_extension_load(duckdb_pglake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
