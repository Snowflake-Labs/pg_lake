-- Upgrade script for pg_lake_iceberg from 3.5 to 3.6

CREATE FUNCTION lake_iceberg.relocate_table(
    table_name regclass,
    new_location text
)
RETURNS text
LANGUAGE C
STRICT
AS 'MODULE_PATHNAME', 'iceberg_relocate_table';

REVOKE ALL ON FUNCTION lake_iceberg.relocate_table(regclass, text) FROM public;
GRANT EXECUTE ON FUNCTION lake_iceberg.relocate_table(regclass, text) TO lake_read_write;
