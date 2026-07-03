/*
 * pg_lake_timeseries--3.4.sql
 *
 * Skeleton install script. Defines the catalog surface and the user-facing
 * function signatures for a time-series table that is stored as a small,
 * indexed heap "delta" merged over a nearly-complete Iceberg "base"
 * (merge-on-read, model A). The behavior is specified in DESIGN.md.
 *
 * NOTE: this is a skeleton. Catalog tables are real; the functions are stubs
 * that raise 'not yet implemented' so the surface can be reviewed and iterated
 * on before the C background worker and CustomScan land.
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_lake_timeseries" to load this file. \quit

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------

CREATE SCHEMA timeseries;
GRANT USAGE ON SCHEMA timeseries TO public;

-- ---------------------------------------------------------------------------
-- Catalog
-- ---------------------------------------------------------------------------

/*
 * One row per managed time-series table. `parent` is the user-facing relation
 * (a view or, later, a CustomScan-backed relation) that unions the heap delta
 * with the Iceberg base. See DESIGN.md "Data model".
 */
CREATE TABLE timeseries.tables (
	parent				regclass PRIMARY KEY,
	-- time (partitioning) column, must be a timestamp/timestamptz
	time_column			name		NOT NULL,
	-- logical key that identifies a row across versions, e.g. {series_id, ts}
	key_columns			name[]		NOT NULL,
	-- fixed-length partition granularity for the delta (hour/day/week); month/
	-- year granularities use date_trunc instead of epoch flooring, see DESIGN.md
	partition_interval	interval	NOT NULL,
	-- how far ahead of now() the frontier worker keeps delta partitions ready
	precreate_ahead		int			NOT NULL DEFAULT 7,
	-- Iceberg base table (a pg_lake_iceberg table), partitioned by days(ts)
	cold_table			regclass	NOT NULL,
	-- ORDER BY list applied when flushing to cold, for tight file min/max
	cluster_columns		text		NOT NULL DEFAULT '',
	-- monotonic per-row version sequence (assigned at insert time)
	seq_sequence		regclass	NOT NULL,
	enabled				boolean		NOT NULL DEFAULT true
);

/*
 * The set of delta partitions that currently hold unflushed rows. Drives the
 * CustomScan's dirty/clean decision: clean partitions push scans and
 * aggregation straight to Iceberg; dirty partitions are reconciled first.
 * See DESIGN.md "Read path".
 */
CREATE TABLE timeseries.delta_partitions (
	parent		regclass	NOT NULL REFERENCES timeseries.tables(parent) ON DELETE CASCADE,
	part_start	timestamptz	NOT NULL,
	part_end	timestamptz	NOT NULL,
	PRIMARY KEY (parent, part_start)
);

SELECT pg_catalog.pg_extension_config_dump('timeseries.tables', '');
SELECT pg_catalog.pg_extension_config_dump('timeseries.delta_partitions', '');

-- ---------------------------------------------------------------------------
-- User-facing API (stubs)
-- ---------------------------------------------------------------------------

/*
 * Register a time-series table. Creates the heap delta (partitioned by
 * time_column), the timed indexes, the Iceberg base table if it does not
 * exist, the version sequence, and the merge-on-read parent relation.
 */
CREATE FUNCTION timeseries.create_table(
	parent				regclass,
	time_column			name,
	key_columns			name[],
	partition_interval	interval,
	hot_retention		interval,
	cold_table			regclass DEFAULT NULL,
	precreate_ahead		int DEFAULT 7,
	cluster_columns		text DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'pg_lake_timeseries: create_table is not yet implemented'
		USING HINT = 'See DESIGN.md, section "Write path".';
END;
$$;
COMMENT ON FUNCTION timeseries.create_table(regclass, name, name[], interval, interval, regclass, int, text)
	IS 'Register a heap-delta-over-Iceberg time-series table (skeleton).';

/*
 * Flush committed delta rows (seq <= safe high-water mark) into the Iceberg
 * base as a merge-on-read (supersede by key, apply tombstones), atomically
 * removing the flushed rows from the delta. Returns the new watermark.
 */
CREATE FUNCTION timeseries.flush(
	parent	regclass,
	hwm		bigint DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'pg_lake_timeseries: flush is not yet implemented'
		USING HINT = 'See DESIGN.md, section "Flush".';
END;
$$;
COMMENT ON FUNCTION timeseries.flush(regclass, bigint)
	IS 'Merge committed delta rows into the Iceberg base (skeleton).';

/*
 * Single maintenance pass for one table: pre-create the delta frontier, drain
 * the DEFAULT catch-all partition, and flush aged delta into Iceberg. This is
 * the body the background worker drives; also callable manually / via pg_cron.
 */
CREATE FUNCTION timeseries.maintain(parent regclass)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'pg_lake_timeseries: maintain is not yet implemented'
		USING HINT = 'See DESIGN.md, section "Frontier & maintenance".';
END;
$$;
COMMENT ON FUNCTION timeseries.maintain(regclass)
	IS 'Frontier pre-create + DEFAULT drain + flush for one table (skeleton).';

/*
 * Compute a safe flush high-water mark: the largest seq such that every seq
 * <= it is guaranteed committed and visible (no in-flight gaps). See DESIGN.md
 * "Correctness".
 */
CREATE FUNCTION timeseries.safe_hwm(parent regclass)
RETURNS bigint
LANGUAGE plpgsql AS $$
BEGIN
	RAISE EXCEPTION 'pg_lake_timeseries: safe_hwm is not yet implemented'
		USING HINT = 'See DESIGN.md, section "Correctness".';
END;
$$;
COMMENT ON FUNCTION timeseries.safe_hwm(regclass)
	IS 'Largest fully-committed version sequence safe to flush (skeleton).';
