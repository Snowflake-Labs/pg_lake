/*
 * pg_lake_log 1.0 – initial schema
 *
 * Creates the pg_lake_log schema and helper objects.  The actual Iceberg
 * target table is created by the user (or by the helper function below)
 * because it requires a cloud storage location.
 */

CREATE SCHEMA pg_lake_log;
GRANT USAGE ON SCHEMA pg_lake_log TO public;


/*
 * pg_lake_log.create_log_table creates a suitably-typed Iceberg table at
 * the specified location and then points pg_lake_log.target_table at it.
 *
 * Example:
 *   SELECT pg_lake_log.create_log_table(
 *       'myschema.pg_logs',
 *       's3://my-bucket/pglake/pg_logs'
 *   );
 */
CREATE OR REPLACE FUNCTION pg_lake_log.create_log_table(
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

	PERFORM set_config('pg_lake_log.target_table', table_name, false);
END;
$$;

COMMENT ON FUNCTION pg_lake_log.create_log_table(text, text) IS
'Create an Iceberg table with the pg_lake_log schema and set it as the capture target.';


/*
 * pg_lake_log.buffer_status returns diagnostic counters from the
 * shared-memory ring buffer.
 */
CREATE OR REPLACE FUNCTION pg_lake_log.buffer_status(
	OUT write_pos      bigint,
	OUT read_pos       bigint,
	OUT buffered_count bigint,
	OUT dropped_count  bigint
)
RETURNS record
LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_log_buffer_status$function$;

COMMENT ON FUNCTION pg_lake_log.buffer_status() IS
'Return diagnostic counters for the pg_lake_log shared-memory ring buffer.';


/*
 * pg_lake_log.flush() synchronously drains the in-memory buffer and writes
 * any pending entries to the Iceberg table.  Useful in tests or when an
 * immediate flush is needed before querying the table.
 */
CREATE OR REPLACE FUNCTION pg_lake_log.flush()
RETURNS bigint
LANGUAGE C
AS 'MODULE_PATHNAME', $function$pg_lake_log_flush$function$;

COMMENT ON FUNCTION pg_lake_log.flush() IS
'Drain the pg_lake_log ring buffer and write pending entries to the Iceberg table. Returns the number of entries written.';
