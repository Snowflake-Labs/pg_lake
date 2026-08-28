-- Upgrade script for pg_lake_iceberg from 3.4 to 3.5

CREATE FUNCTION lake_iceberg.find_all_referenced_files_best_effort(metadata_path text, OUT path text)
	RETURNS SETOF text
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', 'find_all_referenced_files_best_effort';

-- Like find_all_referenced_files, it walks an arbitrary object-store path with
-- the server's credentials, so it must not be world-callable. The VACUUM path
-- calls it as the extension owner, so this does not affect cleanup.
REVOKE ALL ON FUNCTION lake_iceberg.find_all_referenced_files_best_effort(text) FROM public;
