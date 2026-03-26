/*
 * pg_lake_log 1.0 – initial schema
 *
 * Creates the lake_log schema, a metadata table to track target Iceberg
 * tables, and helper functions.  The actual Iceberg target table is created
 * by the user (or by the helper function below) because it requires a
 * cloud storage location.
 */

CREATE SCHEMA lake_log;
GRANT USAGE ON SCHEMA lake_log TO public;


/*
 * lake_log.log_tables tracks which Iceberg tables receive log entries.
 * The background worker reads this table on each flush cycle.
 */
CREATE TABLE lake_log.log_tables (
	table_name regclass NOT NULL,
	PRIMARY KEY (table_name)
);


/*
 * lake_log.create_log_table creates a suitably-typed Iceberg table at
 * the specified location and registers it as a log target.
 *
 * Example:
 *   SELECT lake_log.create_log_table(
 *       'myschema.pg_logs',
 *       's3://my-bucket/pglake/pg_logs'
 *   );
 */
CREATE OR REPLACE FUNCTION lake_log.create_log_table(
	table_name text,
	location   text
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
	EXECUTE format(
		'CREATE TABLE %s ('
		'    log_time  timestamptz NOT NULL,'
		'    pid       int         NOT NULL,'
		'    severity  text        NOT NULL,'
		'    message   text        NOT NULL,'
		'    detail    text,'
		'    context   text'
		') USING iceberg'
		' WITH (location = %L)',
		table_name,
		location
	);

	INSERT INTO lake_log.log_tables (table_name)
	VALUES (table_name::regclass);
END;
$$;

COMMENT ON FUNCTION lake_log.create_log_table(text, text) IS
'Create an Iceberg table with the pg_lake_log schema and register it as a log capture target.';


/*
 * lake_log.buffer_status returns diagnostic counters from the
 * shared-memory ring buffer.
 */
CREATE OR REPLACE FUNCTION lake_log.buffer_status(
	OUT write_pos      bigint,
	OUT read_pos       bigint,
	OUT buffered_count bigint,
	OUT dropped_count  bigint
)
RETURNS record
LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_log_buffer_status$function$;

COMMENT ON FUNCTION lake_log.buffer_status() IS
'Return diagnostic counters for the pg_lake_log shared-memory ring buffer.';


/*
 * lake_log.flush() synchronously drains the in-memory buffer and writes
 * any pending entries to all registered log tables.  Useful in tests or
 * when an immediate flush is needed before querying the table.
 */
CREATE OR REPLACE FUNCTION lake_log.flush()
RETURNS bigint
LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_log_flush$function$;

COMMENT ON FUNCTION lake_log.flush() IS
'Drain the pg_lake_log ring buffer and write pending entries to all registered Iceberg log tables. Returns the number of entries written.';
