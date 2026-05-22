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

# Vendored duckdb-iceberg @ v1.5-variegata, with a local patch (see
# patches/duckdb-iceberg/expose_puffin_dv_functions.patch) that exposes
# Puffin envelope and Deletion Vector primitives as public DuckDB
# table-valued and scalar functions. Pgduck/pg_lake tests consume these
# directly; the future pg_lake_iceberg v3 write/read path will compose
# them via SQL.
duckdb_extension_load(iceberg
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/duckdb-iceberg
)

# Extension from this repo
duckdb_extension_load(duckdb_pglake
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)
