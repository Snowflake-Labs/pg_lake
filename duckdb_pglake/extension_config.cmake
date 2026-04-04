# External extensions to link into libduckdb
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG 7e86e7a5e5a1f01f458361bebdfa9b0a9a73a619
    INCLUDE_DIR src/include
    ADD_PATCHES
)

duckdb_extension_load(aws
    GIT_URL https://github.com/duckdb/duckdb-aws
    GIT_TAG b2649e68341a9ee717588dd23f277904727ce793
)

duckdb_extension_load(azure
    GIT_URL https://github.com/duckdb/duckdb-azure
    GIT_TAG ea6ffae3710ec568ce08579dbfc0cddc8c759227
)

# Extension from this repo
duckdb_extension_load(duckdb_pglake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
