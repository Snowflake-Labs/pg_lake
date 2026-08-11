/*
 * pg_lake_timeseries--3.4.sql
 *
 * Two overlapping storage tiers behind one relation (DESIGN.md section 13):
 *
 *   - the last `hot_retention` worth of time lives in a range-partitioned heap,
 *     indexed and mutable, up to now();
 *   - everything older lives in one internally-partitioned pg_lake Iceberg
 *     table, which also keeps a *lagging* copy of the hot window so external
 *     Iceberg readers see recent data;
 *   - reads are routed by a stored authority boundary B: PostgreSQL is
 *     authoritative for ts >= B, Iceberg for ts < B. Nobody reads the
 *     non-authoritative copy, so the lagging copy needs no invalidation.
 *
 * Mutations that reach a sealed (ts < B) partition land in a per-partition
 * delta and are merged over Iceberg on read until a background repair folds
 * them in.
 *
 * The user-facing relation is a view whose definition is regenerated from the
 * catalog whenever the boundary moves, so the routing predicates are plan-time
 * constants and a single-tier query prunes the other tier away entirely. The
 * CustomScan in DESIGN.md section 5.3 replaces the view later and reads the same
 * catalog without needing DDL -- and, unlike a view, can also specialise the plan
 * on the set of dirty partitions, which a view cannot do because it cannot be
 * replaced while a statement that references it is running.
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
 * (a routing view) that presents the hot heap and the Iceberg table as one
 * relation. See DESIGN.md sections 4.5 and 13.1.
 */
CREATE TABLE timeseries.tables (
	parent				regclass PRIMARY KEY,
	-- hot tier: heap, RANGE-partitioned on time_column
	hot_table			regclass	NOT NULL,
	-- cold tier: pg_lake Iceberg table, partitioned by a time transform
	cold_table			regclass	NOT NULL,
	-- range-partitioned wrapper whose only partition is cold_table, bounded at
	-- the authority boundary: what the routing view reads, so that a hot-window
	-- query prunes the Iceberg scan away at plan time
	cold_scan			regclass	NOT NULL,
	-- overlay for mutations that land below the authority boundary
	delta_table			regclass	NOT NULL,
	-- time (partitioning) column, a timestamp/timestamptz that is NOT NULL
	time_column			name		NOT NULL,
	-- logical key identifying a row across versions, e.g. {series_id, ts};
	-- empty for keyless append-only tables, which cannot be updated
	key_columns			name[]		NOT NULL,
	-- fixed-length partition granularity of the hot tier (hour/day/week)
	partition_interval	interval	NOT NULL,
	-- how much time stays authoritative in PostgreSQL before a partition seals
	hot_retention		interval	NOT NULL,
	-- how much history the Iceberg tier keeps, NULL for unlimited
	cold_retention		interval,
	-- how many partition intervals ahead of now() the frontier is pre-created
	precreate_ahead		int			NOT NULL DEFAULT 7,
	-- ORDER BY list applied when writing to cold, for tight file min/max
	cluster_columns		name[]		NOT NULL DEFAULT '{}',
	-- authority boundary B: PostgreSQL owns ts >= boundary, Iceberg owns ts <
	-- boundary. Advanced only by seal(), which has proven Iceberg completeness.
	boundary			timestamptz	NOT NULL,
	-- whether plain INSERTs upsert on the key in the hot tier
	upsert				boolean		NOT NULL DEFAULT false,
	-- version sequence for delta rows (newest version of a key wins)
	seq_sequence		regclass	NOT NULL,
	enabled				boolean		NOT NULL DEFAULT true
);

/*
 * Per-partition authority (DESIGN.md section 13.4). The boundary in
 * timeseries.tables is derived from this table: it is the smallest part_start
 * that is still 'hot' or 'sealing'.
 *
 * States:
 *   hot         - PostgreSQL authoritative, heap partition exists
 *   sealing     - PostgreSQL authoritative, Iceberg write in flight
 *   cold_clean  - Iceberg authoritative, no overlay, no reverse connection
 *   cold_dirty  - Iceberg authoritative but superseded by delta rows; reads
 *                 merge until repair() folds the delta in
 */
CREATE TABLE timeseries.partitions (
	parent			regclass	NOT NULL REFERENCES timeseries.tables(parent) ON DELETE CASCADE,
	part_start		timestamptz	NOT NULL,
	part_end		timestamptz	NOT NULL,
	state			text		NOT NULL
		CHECK (state IN ('hot', 'sealing', 'cold_clean', 'cold_dirty')),
	-- heap partition backing this range, NULL once the partition sealed
	hot_partition	regclass,
	-- when the lagging Iceberg copy of this range was last refreshed
	synced_at		timestamptz,
	-- when Iceberg became authoritative for this range
	sealed_at		timestamptz,
	PRIMARY KEY (parent, part_start),
	CHECK (part_end > part_start)
);

CREATE INDEX partitions_state_idx ON timeseries.partitions (parent, state);

SELECT pg_catalog.pg_extension_config_dump('timeseries.tables', '');
SELECT pg_catalog.pg_extension_config_dump('timeseries.partitions', '');

-- ---------------------------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------------------------

/*
 * Floor a timestamp to a partition boundary. Only fixed-length intervals are
 * supported: month/year granularities would need date_trunc and are rejected
 * by create_table.
 */
CREATE FUNCTION timeseries.partition_start(ts timestamptz, part_interval interval)
RETURNS timestamptz
LANGUAGE sql IMMUTABLE STRICT AS $$
	SELECT to_timestamp(floor(extract(epoch FROM ts) / extract(epoch FROM part_interval))
						* extract(epoch FROM part_interval))
$$;
COMMENT ON FUNCTION timeseries.partition_start(timestamptz, interval)
	IS 'Floor a timestamp to a fixed-length partition boundary.';

/* The columns of a relation, in attribute order. */
CREATE FUNCTION timeseries.column_names(rel regclass)
RETURNS name[]
LANGUAGE sql STABLE STRICT AS $$
	SELECT array_agg(attname ORDER BY attnum)
	  FROM pg_catalog.pg_attribute
	 WHERE attrelid = rel AND attnum > 0 AND NOT attisdropped
$$;

/* "a, b, c" from {a,b,c}, each identifier quoted. */
CREATE FUNCTION timeseries.quoted_list(cols name[])
RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
	SELECT string_agg(quote_ident(col), ', ' ORDER BY ord)
	  FROM unnest(cols) WITH ORDINALITY AS u(col, ord)
$$;

/* "<lhs>.a = <rhs>.a AND ..." for joining on the logical key. */
CREATE FUNCTION timeseries.key_join_clause(key_columns name[], lhs text, rhs text)
RETURNS text
LANGUAGE sql IMMUTABLE STRICT AS $$
	SELECT string_agg(format('%s.%I = %s.%I', lhs, col, rhs, col), ' AND ' ORDER BY ord)
	  FROM unnest(key_columns) WITH ORDINALITY AS u(col, ord)
$$;

/* The authority boundary of a table. */
CREATE FUNCTION timeseries.boundary(parent regclass)
RETURNS timestamptz
LANGUAGE sql STABLE STRICT AS $$
	SELECT boundary FROM timeseries.tables t WHERE t.parent = $1
$$;
COMMENT ON FUNCTION timeseries.boundary(regclass)
	IS 'Authority boundary B: PostgreSQL owns ts >= B, Iceberg owns ts < B.';

/* Whether the current user owns (or is a member of the role owning) a relation. */
CREATE FUNCTION timeseries.is_owner(rel regclass)
RETURNS boolean
LANGUAGE sql STABLE STRICT AS $$
	SELECT pg_catalog.pg_has_role(current_user, c.relowner, 'USAGE')
	  FROM pg_catalog.pg_class c WHERE c.oid = rel
$$;

/* Raise unless the caller owns the table (all mutating API functions). */
CREATE FUNCTION timeseries.check_owner(rel regclass)
RETURNS void
LANGUAGE plpgsql STRICT AS $$
BEGIN
	IF NOT coalesce(timeseries.is_owner(rel), false) THEN
		RAISE EXCEPTION 'permission denied for relation %', rel::text
			USING ERRCODE = 'insufficient_privilege';
	END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- Routing view generation
-- ---------------------------------------------------------------------------

/*
 * Re-bound the cold-tier wrapper on the authority boundary.
 *
 * The wrapper is a range-partitioned table whose only partition is the Iceberg
 * table, attached FROM (MINVALUE) TO (boundary). Its bound is what makes tier
 * elimination reliable: partition pruning removes the Iceberg scan from a
 * ts >= boundary query at plan time, whatever the shape of the query, whereas
 * refuting the branch's own WHERE clause needs both the UNION ALL to be
 * flattened (which the planner only does for SELECT *) and constraint_exclusion
 * to be raised above its default.
 *
 * The bound is not a filter -- PostgreSQL does not enforce partition bounds on
 * foreign tables, and the Iceberg table deliberately holds a lagging copy of
 * rows above the boundary -- so the view still carries its own ts < boundary
 * predicate. The bound only ever tells the planner what it may skip.
 */
CREATE FUNCTION timeseries.rebound_cold(parent regclass, new_boundary timestamptz)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
	t record;
BEGIN
	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = rebound_cold.parent;

	EXECUTE format('ALTER TABLE %s DETACH PARTITION %s',
				   t.cold_scan::text, t.cold_table::text);
	EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (MINVALUE) TO (%L)',
				   t.cold_scan::text, t.cold_table::text, new_boundary);
END;
$$;

/*
 * Regenerate the routing view from catalog state.
 *
 * The boundary is emitted as a literal so the planner can see it as a constant:
 * a query restricted to one side of it has the other branch pruned (the hot tier
 * by its own range partitioning, the cold tier by the bound of the cold-scan
 * wrapper) and never reaches it.
 *
 * The shape of the view depends on the boundary and on the key, and on nothing
 * else -- in particular not on which partitions are dirty. It has to: a view
 * cannot be replaced while a statement that references it is running, so the
 * write path (an INSTEAD OF trigger on this very view) is in no position to
 * change it. The delta overlay is therefore permanent, and the price of a write
 * below the boundary being visible immediately is that every cold-tier read
 * carries an anti-join against the delta. The delta is empty on a repaired
 * table, so that anti-join is a probe into an empty hash table; what it does
 * cost is the chance to push the cold branch down as a whole query.
 *
 * Called on a boundary advance, which only ever happens in seal() -- outside of
 * any statement that reads the view.
 */
CREATE FUNCTION timeseries.refresh_view(parent regclass)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
	t				record;
	cols			text;
	alias_cols		text;
	ts_col			text;
	view_sql		text;
BEGIN
	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = refresh_view.parent;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a pg_lake_timeseries table', parent::text;
	END IF;

	cols := timeseries.quoted_list(timeseries.column_names(t.hot_table));
	ts_col := quote_ident(t.time_column);

	/* the hot tier owns everything at or above the boundary */
	view_sql := format('SELECT %s FROM %s WHERE %s >= %L',
					   cols, t.hot_table::text, ts_col, t.boundary);

	IF cardinality(t.key_columns) = 0 THEN
		/*
		 * A keyless table rejects updates and deletes, so nothing in the delta
		 * can supersede an Iceberg row: the two cold sources are disjoint and
		 * are simply concatenated.
		 */
		view_sql := view_sql || format(
			' UNION ALL '
			'SELECT %s FROM %s WHERE %s < %L'
			' UNION ALL '
			'SELECT %s FROM %s WHERE %s < %L',
			cols, t.cold_scan::text, ts_col, t.boundary,
			cols, t.delta_table::text, ts_col, t.boundary);
	ELSE
		alias_cols := (SELECT string_agg('c.' || quote_ident(col), ', ' ORDER BY ord)
						 FROM unnest(timeseries.column_names(t.hot_table))
							  WITH ORDINALITY AS u(col, ord));

		/*
		 * Iceberg rows whose key was not superseded, then the newest live
		 * version of each key in the delta. Tombstones drop out of the second
		 * branch and mask their Iceberg row through the first.
		 */
		view_sql := view_sql || format(
			' UNION ALL '
			'SELECT %s FROM %s c WHERE c.%s < %L'
			' AND NOT EXISTS (SELECT 1 FROM %s d WHERE %s)'
			' UNION ALL '
			'SELECT %s FROM (SELECT DISTINCT ON (%s) * FROM %s WHERE %s < %L'
			' ORDER BY %s, _ts_seq DESC) l WHERE NOT l._ts_deleted',
			alias_cols, t.cold_scan::text, ts_col, t.boundary,
			t.delta_table::text, timeseries.key_join_clause(t.key_columns, 'd', 'c'),
			cols, timeseries.quoted_list(t.key_columns), t.delta_table::text,
			ts_col, t.boundary, timeseries.quoted_list(t.key_columns));
	END IF;

	EXECUTE format('CREATE OR REPLACE VIEW %s AS %s', parent::text, view_sql);
END;
$$;
COMMENT ON FUNCTION timeseries.refresh_view(regclass)
	IS 'Regenerate the tier-routing view from catalog state.';

-- ---------------------------------------------------------------------------
-- Hot partition frontier
-- ---------------------------------------------------------------------------

/*
 * Create hot partitions so that every timestamp up to `upto` has one, and
 * register them as authoritative for PostgreSQL.
 *
 * Partitions are created ahead of the writers (precreate_ahead intervals past
 * now() by default), so the insert path never has to run DDL. There is no
 * DEFAULT partition: a timestamp beyond the frontier is a bug in maintenance,
 * not something to silently absorb, and a DEFAULT partition would block
 * attaching the next range.
 */
CREATE FUNCTION timeseries.add_partitions(parent regclass, upto timestamptz DEFAULT NULL)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
	t			record;
	part_start	timestamptz;
	part_end	timestamptz;
	part_name	text;
	created		int := 0;
BEGIN
	PERFORM timeseries.check_owner(parent);

	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = add_partitions.parent;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a pg_lake_timeseries table', parent::text;
	END IF;

	upto := coalesce(upto, now() + t.precreate_ahead * t.partition_interval);

	/* start at the frontier, or at the boundary for a fresh table */
	SELECT max(p.part_end) INTO part_start
	  FROM timeseries.partitions p WHERE p.parent = add_partitions.parent;

	part_start := coalesce(part_start, t.boundary);

	WHILE part_start <= upto LOOP
		part_end := part_start + t.partition_interval;

		SELECT format('%I.%I', n.nspname,
					  left(c.relname, 40) || '_' ||
					  to_char(part_start AT TIME ZONE 'UTC', 'YYYYMMDD"t"HH24MI'))
		  INTO part_name
		  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE c.oid = t.hot_table;

		EXECUTE format('CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
					   part_name, t.hot_table::text, part_start, part_end);

		INSERT INTO timeseries.partitions (parent, part_start, part_end, state, hot_partition)
		VALUES (add_partitions.parent, part_start, part_end, 'hot', part_name::regclass);

		part_start := part_end;
		created := created + 1;
	END LOOP;

	RETURN created;
END;
$$;
COMMENT ON FUNCTION timeseries.add_partitions(regclass, timestamptz)
	IS 'Extend the hot partition frontier up to a point in time.';

-- ---------------------------------------------------------------------------
-- create_table / drop_table
-- ---------------------------------------------------------------------------

/*
 * Convert an empty ordinary table into a two-tier time-series table.
 *
 * The relation is used as the schema template and then replaced by the routing
 * view of the same name, so existing queries keep working. Alongside it we
 * create:
 *
 *   <name>_hot    RANGE-partitioned heap, authoritative for ts >= boundary
 *   <name>_cold   Iceberg table, authoritative for ts < boundary
 *   <name>_delta  overlay for mutations that land below the boundary
 *   <name>_seq    version sequence for delta rows
 *
 * The boundary starts at the beginning of the hot window: PostgreSQL is
 * authoritative for the last hot_retention worth of time (partitions covering
 * it are created immediately, so writes anywhere in the window go to the heap),
 * and the initially empty Iceberg tier owns everything before it. Bulk history
 * is therefore loaded by writing to the cold table directly; rows written
 * through the view below the boundary are correct but take the slower
 * delta + repair path.
 */
CREATE FUNCTION timeseries.create_table(
	relation			regclass,
	time_column			name,
	key_columns			name[] DEFAULT NULL,
	partition_interval	interval DEFAULT interval '1 day',
	hot_retention		interval DEFAULT interval '7 days',
	cold_retention		interval DEFAULT NULL,
	upsert				boolean DEFAULT false,
	cold_table			regclass DEFAULT NULL,
	cold_location		text DEFAULT NULL,
	precreate_ahead		int DEFAULT 7,
	cluster_columns		name[] DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
	rel_schema		name;
	rel_name		name;
	hot_table		text;
	delta_table		text;
	cold_name		text;
	cold_scan_name	text;
	seq_name		text;
	all_columns		name[];
	col_defs		text;
	time_type		text;
	transform		text;
	boundary		timestamptz;
	key_cols		name[] := coalesce(key_columns, '{}'::name[]);
	cluster_cols	name[] := coalesce(cluster_columns, '{}'::name[]);
	with_options	text;
	any_row			boolean;
BEGIN
	PERFORM timeseries.check_owner(relation);

	SELECT n.nspname, c.relname INTO rel_schema, rel_name
	  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
	 WHERE c.oid = relation AND c.relkind = 'r';

	IF NOT FOUND THEN
		RAISE EXCEPTION '% must be an ordinary table', relation::text
			USING HINT = 'Create an empty table with the desired columns first.';
	END IF;

	EXECUTE format('SELECT EXISTS (SELECT 1 FROM %s)', relation::text) INTO any_row;
	IF any_row THEN
		RAISE EXCEPTION '% is not empty', relation::text
			USING HINT = 'Load history into the Iceberg tier after conversion.';
	END IF;

	/* the time column must be a NOT NULL-able timestamp we can floor */
	SELECT format_type(atttypid, atttypmod) INTO time_type
	  FROM pg_attribute
	 WHERE attrelid = relation AND attname = time_column AND attnum > 0 AND NOT attisdropped;

	IF time_type IS NULL THEN
		RAISE EXCEPTION 'column %s does not exist in %s', quote_ident(time_column), relation::text;
	ELSIF time_type NOT IN ('timestamp with time zone', 'timestamp without time zone') THEN
		RAISE EXCEPTION 'time column %s must be a timestamp, not %s',
						quote_ident(time_column), time_type;
	END IF;

	IF extract(month FROM partition_interval) <> 0
		OR extract(year FROM partition_interval) <> 0
		OR extract(epoch FROM partition_interval) <= 0 THEN
		RAISE EXCEPTION 'partition_interval must be a positive fixed-length interval'
			USING HINT = 'Use hour/day/week granularities; month and year are not supported yet.';
	END IF;

	IF hot_retention < partition_interval THEN
		RAISE EXCEPTION 'hot_retention must be at least one partition_interval';
	END IF;

	all_columns := timeseries.column_names(relation);

	IF EXISTS (SELECT 1 FROM unnest(all_columns) col WHERE col LIKE '\_ts\_%') THEN
		RAISE EXCEPTION 'column names starting with _ts_ are reserved by pg_lake_timeseries';
	END IF;

	IF cardinality(key_cols) > 0 THEN
		IF NOT (key_cols <@ all_columns) THEN
			RAISE EXCEPTION 'key_columns must be columns of %', relation::text;
		END IF;

		/*
		 * The key must contain the time column: it is the partition key of the
		 * hot tier, and PostgreSQL requires a unique index on a partitioned
		 * table to include the partition key.
		 */
		IF NOT (time_column = ANY (key_cols)) THEN
			RAISE EXCEPTION 'key_columns must include the time column %s',
							quote_ident(time_column);
		END IF;
	ELSIF upsert THEN
		RAISE EXCEPTION 'upsert requires key_columns';
	END IF;

	/*
	 * cluster_columns is interpolated into the ORDER BY of the Iceberg write
	 * performed by the maintenance worker, which runs as a superuser: it is kept
	 * as an identifier array and validated here rather than accepted as free
	 * text.
	 */
	IF NOT (cluster_cols <@ all_columns) THEN
		RAISE EXCEPTION 'cluster_columns must be columns of %', relation::text;
	END IF;

	hot_table := format('%I.%I', rel_schema, rel_name || '_hot');
	delta_table := format('%I.%I', rel_schema, rel_name || '_delta');
	cold_name := format('%I.%I', rel_schema, rel_name || '_cold');
	cold_scan_name := format('%I.%I', rel_schema, rel_name || '_cold_scan');
	seq_name := format('%I.%I', rel_schema, rel_name || '_seq');

	/* hot tier */
	EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS INCLUDING COMMENTS'
				   ' INCLUDING STORAGE) PARTITION BY RANGE (%I)',
				   hot_table, relation::text, time_column);
	EXECUTE format('ALTER TABLE %s ALTER COLUMN %I SET NOT NULL', hot_table, time_column);

	IF cardinality(key_cols) > 0 THEN
		EXECUTE format('CREATE UNIQUE INDEX ON %s (%s)',
					   hot_table, timeseries.quoted_list(key_cols));
	END IF;

	/* a BRIN-friendly access path for time ranges when ts does not lead the key */
	IF cardinality(key_cols) = 0 OR key_cols[1] <> time_column THEN
		EXECUTE format('CREATE INDEX ON %s (%I)', hot_table, time_column);
	END IF;

	/* delta overlay: the user columns plus version + tombstone */
	EXECUTE format('CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS,'
				   ' _ts_seq bigint NOT NULL, _ts_deleted boolean NOT NULL DEFAULT false)',
				   delta_table, relation::text);

	IF cardinality(key_cols) > 0 THEN
		EXECUTE format('CREATE INDEX ON %s (%s, _ts_seq DESC)',
					   delta_table, timeseries.quoted_list(key_cols));
	END IF;

	EXECUTE format('CREATE INDEX ON %s (%I)', delta_table, time_column);

	/* cold tier */
	SELECT string_agg(format('%I %s', attname, format_type(atttypid, atttypmod)),
					  ', ' ORDER BY attnum)
	  INTO col_defs
	  FROM pg_attribute
	 WHERE attrelid = relation AND attnum > 0 AND NOT attisdropped;

	boundary := timeseries.partition_start(now() - hot_retention, partition_interval);

	IF cold_table IS NULL THEN
		/*
		 * The Iceberg partition transform is chosen so that a partition range
		 * covers whole Iceberg partitions: sync() and repair() overwrite a range
		 * with DELETE + INSERT, and pg_lake turns a DELETE that matches whole
		 * partitions into a metadata-only file removal instead of writing
		 * position deletes. day(ts) covers any whole-day interval; a sub-hour
		 * partition_interval necessarily lands inside an hour partition and does
		 * pay for position deletes on re-sync.
		 */
		transform := CASE
						WHEN partition_interval <= interval '1 hour' THEN 'hour'
						ELSE 'day'
					 END;

		with_options := format('partition_by = %L', format('%s(%I)', transform, time_column));

		IF cold_location IS NOT NULL THEN
			with_options := with_options || format(', location = %L', cold_location);
		END IF;

		EXECUTE format('CREATE TABLE %s (%s) USING iceberg WITH (%s)',
					   cold_name, col_defs, with_options);
	ELSE
		cold_name := cold_table::text;
	END IF;

	/*
	 * The routing view reads the cold tier through a range-partitioned wrapper
	 * rather than directly. Its bound is what makes tier elimination reliable:
	 * partition pruning removes the Iceberg scan from a ts >= boundary query at
	 * plan time, whatever the shape of the query. Refuting the cold branch's own
	 * ts < boundary predicate would need constraint_exclusion = on, which is not
	 * the default and which we do not want to impose on the whole session.
	 *
	 * The bound is a pruning hint only: PostgreSQL neither enforces nor applies
	 * partition constraints as filters for a foreign table, so the view keeps its
	 * explicit ts < boundary predicate to mask the lagging Iceberg copy of the
	 * hot window.
	 */
	EXECUTE format('CREATE TABLE %s (%s) PARTITION BY RANGE (%I)',
				   cold_scan_name, col_defs, time_column);
	EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (MINVALUE) TO (%L)',
				   cold_scan_name, cold_name, boundary);

	EXECUTE format('CREATE SEQUENCE %s', seq_name);

	/*
	 * Replace the template relation with the routing view. A placeholder
	 * definition establishes the view (and its column types) so the catalog row
	 * can reference it; refresh_view() then writes the real definition.
	 */
	EXECUTE format('DROP TABLE %s', relation::text);
	EXECUTE format('CREATE VIEW %I.%I AS SELECT %s FROM %s WHERE false',
				   rel_schema, rel_name, timeseries.quoted_list(all_columns), hot_table);

	INSERT INTO timeseries.tables (
		parent, hot_table, cold_table, cold_scan, delta_table, time_column, key_columns,
		partition_interval, hot_retention, cold_retention, precreate_ahead,
		cluster_columns, boundary, upsert, seq_sequence)
	VALUES (
		format('%I.%I', rel_schema, rel_name)::regclass, hot_table::regclass,
		cold_name::regclass, cold_scan_name::regclass, delta_table::regclass,
		time_column, key_cols,
		partition_interval, hot_retention, cold_retention, precreate_ahead,
		cluster_cols, boundary, upsert, seq_name::regclass);

	PERFORM timeseries.refresh_view(format('%I.%I', rel_schema, rel_name)::regclass);

	EXECUTE format('CREATE TRIGGER route_insert INSTEAD OF INSERT ON %I.%I'
				   ' FOR EACH ROW EXECUTE FUNCTION timeseries.route_write()',
				   rel_schema, rel_name);
	EXECUTE format('CREATE TRIGGER route_update INSTEAD OF UPDATE ON %I.%I'
				   ' FOR EACH ROW EXECUTE FUNCTION timeseries.route_write()',
				   rel_schema, rel_name);
	EXECUTE format('CREATE TRIGGER route_delete INSTEAD OF DELETE ON %I.%I'
				   ' FOR EACH ROW EXECUTE FUNCTION timeseries.route_write()',
				   rel_schema, rel_name);

	PERFORM timeseries.add_partitions(format('%I.%I', rel_schema, rel_name)::regclass);
END;
$$;
COMMENT ON FUNCTION timeseries.create_table(regclass, name, name[], interval, interval, interval,
											boolean, regclass, text, int, name[])
	IS 'Convert an empty table into a hot-heap-over-Iceberg time-series table.';

/*
 * Unregister a time-series table. The view is dropped; the tier relations are
 * kept unless drop_data is set, so that the Iceberg data survives by default.
 */
CREATE FUNCTION timeseries.drop_table(parent regclass, drop_data boolean DEFAULT false)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
	t record;
BEGIN
	PERFORM timeseries.check_owner(parent);

	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = drop_table.parent;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a pg_lake_timeseries table', parent::text;
	END IF;

	DELETE FROM timeseries.tables WHERE tables.parent = drop_table.parent;

	EXECUTE format('DROP VIEW %s', t.parent::text);

	/*
	 * The Iceberg table is detached before the wrapper goes away: dropping a
	 * partitioned table takes its partitions with it, and the cold tier may be a
	 * pre-existing table the caller owns and wants to keep.
	 */
	EXECUTE format('ALTER TABLE %s DETACH PARTITION %s',
				   t.cold_scan::text, t.cold_table::text);
	EXECUTE format('DROP TABLE %s', t.cold_scan::text);

	IF drop_data THEN
		EXECUTE format('DROP TABLE %s', t.hot_table::text);
		EXECUTE format('DROP TABLE %s', t.delta_table::text);
		EXECUTE format('DROP TABLE %s', t.cold_table::text);
		EXECUTE format('DROP SEQUENCE %s', t.seq_sequence::text);
	END IF;
END;
$$;
COMMENT ON FUNCTION timeseries.drop_table(regclass, boolean)
	IS 'Unregister a time-series table, optionally dropping its tiers.';

-- ---------------------------------------------------------------------------
-- Write routing
-- ---------------------------------------------------------------------------

/*
 * Mark the partition containing `ts` as superseded by delta rows.
 *
 * A partition row is created on demand: writes can reach ranges that were never
 * hot in this installation (history loaded straight into Iceberg, or a table
 * converted long after the data was written).
 *
 * Only a clean -> dirty transition regenerates the view; further writes into an
 * already-dirty partition are pure DML.
 */
CREATE FUNCTION timeseries.mark_dirty(parent regclass, ts timestamptz)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
	t			record;
	pstart		timestamptz;
BEGIN
	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = mark_dirty.parent;

	pstart := timeseries.partition_start(ts, t.partition_interval);

	UPDATE timeseries.partitions p
	   SET state = 'cold_dirty'
	 WHERE p.parent = mark_dirty.parent AND p.part_start = pstart
	   AND p.state = 'cold_clean';

	IF NOT EXISTS (SELECT 1 FROM timeseries.partitions p
					WHERE p.parent = mark_dirty.parent AND p.part_start = pstart) THEN
		INSERT INTO timeseries.partitions (parent, part_start, part_end, state, sealed_at)
		VALUES (mark_dirty.parent, pstart, pstart + t.partition_interval,
				'cold_dirty', now())
		ON CONFLICT DO NOTHING;
	END IF;
END;
$$;

/*
 * INSTEAD OF trigger on the routing view: send the row to the tier that is
 * authoritative for its timestamp.
 *
 * The boundary is read with FOR SHARE, which is what makes concurrent seals
 * safe: seal() takes FOR NO KEY UPDATE on the same catalog row before it copies
 * a partition into Iceberg, so a writer either commits before the copy starts
 * (and is copied along), or blocks and then re-reads the advanced boundary and
 * routes into the delta instead. There is no window in which a row lands in a
 * partition that is about to be dropped.
 *
 * SECURITY DEFINER because the tiers are implementation detail: a user who was
 * granted INSERT on the view alone still has to be able to write into the heap,
 * bump the version sequence, and -- when the write lands below the boundary --
 * record the partition as dirty in the extension catalog. The relations touched
 * are derived from TG_RELID, and TG_RELID has to be a registered parent view, so
 * the function cannot be aimed at anything the caller does not already reach
 * through that view.
 */
CREATE FUNCTION timeseries.route_write()
RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
DECLARE
	t				record;
	old_ts			timestamptz;
	new_ts			timestamptz;
	set_clause		text;
BEGIN
	SELECT * INTO t FROM timeseries.tables
	 WHERE tables.parent = TG_RELID::regclass FOR SHARE;

	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a pg_lake_timeseries table', TG_RELID::regclass::text;
	END IF;

	IF TG_OP IN ('UPDATE', 'DELETE') AND cardinality(t.key_columns) = 0 THEN
		RAISE EXCEPTION 'cannot % rows of keyless time-series table %',
						lower(TG_OP), t.parent::text
			USING HINT = 'Recreate the table with key_columns to allow updates.';
	END IF;

	IF TG_OP <> 'INSERT' THEN
		EXECUTE format('SELECT ($1).%I', t.time_column) INTO old_ts USING OLD;
	END IF;

	IF TG_OP <> 'DELETE' THEN
		EXECUTE format('SELECT ($1).%I', t.time_column) INTO new_ts USING NEW;

		IF new_ts IS NULL THEN
			RAISE EXCEPTION 'time column %s cannot be NULL', quote_ident(t.time_column);
		END IF;
	END IF;

	/*
	 * Remove the old version first, so that an UPDATE that moves a row across
	 * the boundary (or across partitions) does not leave the old version
	 * behind.
	 */
	IF TG_OP IN ('UPDATE', 'DELETE') THEN
		IF old_ts >= t.boundary THEN
			EXECUTE format('DELETE FROM %s target WHERE %s',
						   t.hot_table::text,
						   timeseries.key_join_clause(t.key_columns, 'target', '($1)'))
				USING OLD;
		ELSE
			/* tombstone the Iceberg version; repair() folds it in later */
			EXECUTE format('INSERT INTO %s SELECT ($1).*, nextval(%L), true',
						   t.delta_table::text, t.seq_sequence::text) USING OLD;

			PERFORM timeseries.mark_dirty(t.parent, old_ts);
		END IF;
	END IF;

	IF TG_OP IN ('INSERT', 'UPDATE') THEN
		IF new_ts >= t.boundary THEN
			IF t.upsert AND TG_OP = 'INSERT' THEN
				SELECT string_agg(format('%I = EXCLUDED.%I', attname, attname), ', ')
				  INTO set_clause
				  FROM pg_attribute
				 WHERE attrelid = t.hot_table AND attnum > 0 AND NOT attisdropped
				   AND NOT (attname = ANY (t.key_columns));

				IF set_clause IS NULL THEN
					EXECUTE format('INSERT INTO %s SELECT ($1).* ON CONFLICT (%s) DO NOTHING',
								   t.hot_table::text,
								   timeseries.quoted_list(t.key_columns)) USING NEW;
				ELSE
					EXECUTE format('INSERT INTO %s SELECT ($1).* ON CONFLICT (%s) DO UPDATE SET %s',
								   t.hot_table::text,
								   timeseries.quoted_list(t.key_columns),
								   set_clause) USING NEW;
				END IF;
			ELSE
				EXECUTE format('INSERT INTO %s SELECT ($1).*', t.hot_table::text) USING NEW;
			END IF;
		ELSE
			EXECUTE format('INSERT INTO %s SELECT ($1).*, nextval(%L), false',
						   t.delta_table::text, t.seq_sequence::text) USING NEW;

			PERFORM timeseries.mark_dirty(t.parent, new_ts);
		END IF;
	END IF;

	IF TG_OP = 'DELETE' THEN
		RETURN OLD;
	END IF;

	RETURN NEW;
END;
$$;
COMMENT ON FUNCTION timeseries.route_write()
	IS 'INSTEAD OF trigger routing writes to the authoritative tier.';

-- ---------------------------------------------------------------------------
-- Cold tier maintenance
-- ---------------------------------------------------------------------------

/*
 * Overwrite one time range of the Iceberg tier with the contents of `source`.
 *
 * The DELETE matches whole Iceberg partitions (see the transform choice in
 * create_table), so it is a metadata-only removal of the data files rather than
 * a write of position deletes. That is what makes rematerialisation cheap enough
 * to be the only mechanism this extension uses to change cold data.
 */
CREATE FUNCTION timeseries.copy_range(parent regclass, part_start timestamptz,
									  part_end timestamptz, source regclass)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
	t			record;
	cols		text;
	order_by	text := '';
	copied		bigint;
BEGIN
	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = copy_range.parent;

	cols := timeseries.quoted_list(timeseries.column_names(t.hot_table));

	IF cardinality(t.cluster_columns) > 0 THEN
		order_by := ' ORDER BY ' || timeseries.quoted_list(t.cluster_columns);
	END IF;

	EXECUTE format('DELETE FROM %s WHERE %I >= %L AND %I < %L',
				   t.cold_table::text, t.time_column, part_start,
				   t.time_column, part_end);

	EXECUTE format('INSERT INTO %s (%s) SELECT %s FROM %s%s',
				   t.cold_table::text, cols, cols, source::text, order_by);

	GET DIAGNOSTICS copied = ROW_COUNT;

	RETURN copied;
END;
$$;

/*
 * Refresh the Iceberg copy of hot partitions that are entirely in the past.
 *
 * This is what gives external Iceberg readers "everything up to last night"
 * without waiting for the partition to leave the hot window: the rows stay
 * authoritative in PostgreSQL (the boundary does not move), so the copy is
 * invisible to queries through the view and can be redone at any time.
 *
 * A partition is synced once, after it stops receiving in-order writes. Later
 * mutations of a still-hot partition are picked up by the re-sync in seal();
 * until then external readers see the older Iceberg copy. Readers that need
 * stricter freshness should query the table through PostgreSQL.
 */
CREATE FUNCTION timeseries.sync(parent regclass, only_start timestamptz DEFAULT NULL)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
	t			record;
	p			record;
	synced		int := 0;
BEGIN
	PERFORM timeseries.check_owner(parent);

	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = sync.parent;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a pg_lake_timeseries table', parent::text;
	END IF;

	FOR p IN
		SELECT * FROM timeseries.partitions part
		 WHERE part.parent = sync.parent AND part.state = 'hot'
		   AND part.part_end <= now()
		   AND (only_start IS NULL OR part.part_start = only_start)
		   AND (only_start IS NOT NULL OR part.synced_at IS NULL
				OR part.synced_at < part.part_end)
		 ORDER BY part.part_start
	LOOP
		PERFORM timeseries.copy_range(parent, p.part_start, p.part_end, p.hot_partition);

		UPDATE timeseries.partitions
		   SET synced_at = now()
		 WHERE partitions.parent = sync.parent AND partitions.part_start = p.part_start;

		synced := synced + 1;
	END LOOP;

	RETURN synced;
END;
$$;
COMMENT ON FUNCTION timeseries.sync(regclass, timestamptz)
	IS 'Refresh the (non-authoritative) Iceberg copy of past hot partitions.';

/*
 * Hand partitions that aged out of the hot window over to Iceberg and advance
 * the authority boundary.
 *
 * Sealing is the only operation that moves the boundary, and it does so only
 * after the range has been proven to be in Iceberg in the same transaction that
 * drops the heap partition. A crash or error at any point rolls the whole thing
 * back: the boundary never advances past data that is not in the cold tier.
 *
 * Concurrency: the catalog row is locked FOR NO KEY UPDATE before the copy, and
 * route_write() reads the boundary FOR SHARE. A writer therefore either commits
 * before the copy starts and is copied along, or waits and then re-reads the
 * advanced boundary and writes to the delta. No row can land in a heap partition
 * that this transaction is about to drop.
 */
CREATE FUNCTION timeseries.seal(parent regclass, upto timestamptz DEFAULT NULL)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
	t				record;
	p				record;
	seal_upto		timestamptz;
	new_boundary	timestamptz;
	sealed			int := 0;
BEGIN
	PERFORM timeseries.check_owner(parent);

	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = seal.parent
		FOR NO KEY UPDATE;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a pg_lake_timeseries table', parent::text;
	END IF;

	/*
	 * Partitions that end before the hot window may be sealed. `upto` moves that
	 * point explicitly, which is how a table is shrunk on demand instead of
	 * waiting for the retention interval to elapse.
	 */
	seal_upto := timeseries.partition_start(coalesce(upto, now() - t.hot_retention),
											t.partition_interval);
	new_boundary := t.boundary;

	FOR p IN
		SELECT * FROM timeseries.partitions part
		 WHERE part.parent = seal.parent AND part.state = 'hot'
		   AND part.part_end <= seal_upto
		 ORDER BY part.part_start
	LOOP
		/*
		 * Seal contiguously upward from the boundary. A gap would mean the
		 * boundary could not be advanced past it without claiming authority
		 * over a range that was never sealed, so stop and let the next pass
		 * retry once the missing partition is dealt with.
		 */
		IF p.part_start <> new_boundary THEN
			RAISE WARNING 'gap in the hot partitions of % at %, stopping seal',
						  parent::text, new_boundary;
			EXIT;
		END IF;

		UPDATE timeseries.partitions
		   SET state = 'sealing'
		 WHERE partitions.parent = seal.parent AND partitions.part_start = p.part_start;

		PERFORM timeseries.copy_range(parent, p.part_start, p.part_end, p.hot_partition);

		EXECUTE format('DROP TABLE %s', p.hot_partition::text);

		UPDATE timeseries.partitions
		   SET state = 'cold_clean', hot_partition = NULL,
			   synced_at = now(), sealed_at = now()
		 WHERE partitions.parent = seal.parent AND partitions.part_start = p.part_start;

		new_boundary := p.part_end;
		sealed := sealed + 1;
	END LOOP;

	IF sealed > 0 THEN
		UPDATE timeseries.tables SET boundary = new_boundary
		 WHERE tables.parent = seal.parent;

		PERFORM timeseries.rebound_cold(parent, new_boundary);
		PERFORM timeseries.refresh_view(parent);
	END IF;

	RETURN sealed;
END;
$$;
COMMENT ON FUNCTION timeseries.seal(regclass, timestamptz)
	IS 'Move aged-out partitions to Iceberg and advance the authority boundary.';

/*
 * Fold the delta back into Iceberg for partitions that were mutated below the
 * boundary, and return them to the cold_clean state.
 *
 * The partition is rematerialised rather than patched: the merged contents are
 * staged, the range is overwritten, and the delta rows for the range are
 * dropped. No Iceberg delete files are produced, and the view loses a branch.
 *
 * Concurrency is the mirror image of seal(): the catalog row is locked, so a
 * concurrent write below the boundary either lands in the delta before the merge
 * reads it, or waits and re-dirties the partition afterwards.
 */
CREATE FUNCTION timeseries.repair(parent regclass, only_start timestamptz DEFAULT NULL)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
	t			record;
	p			record;
	cols		text;
	alias_cols	text;
	key_clause	text;
	repaired	int := 0;
BEGIN
	PERFORM timeseries.check_owner(parent);

	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = repair.parent
		FOR NO KEY UPDATE;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a pg_lake_timeseries table', parent::text;
	END IF;

	IF cardinality(t.key_columns) = 0 THEN
		/* nothing can be dirty without a key: writes below B are rejected */
		RETURN 0;
	END IF;

	cols := timeseries.quoted_list(timeseries.column_names(t.hot_table));
	key_clause := timeseries.key_join_clause(t.key_columns, 'c', 'd');

	SELECT string_agg('c.' || quote_ident(col), ', ' ORDER BY ord)
	  INTO alias_cols
	  FROM unnest(timeseries.column_names(t.hot_table)) WITH ORDINALITY AS u(col, ord);

	FOR p IN
		SELECT * FROM timeseries.partitions part
		 WHERE part.parent = repair.parent AND part.state = 'cold_dirty'
		   AND (only_start IS NULL OR part.part_start = only_start)
		 ORDER BY part.part_start
	LOOP
		/* stage the merged result: the range is overwritten in place below */
		EXECUTE format(
			'CREATE TEMP TABLE pg_temp._ts_repair AS'
			' SELECT %s FROM %s c'
			'  WHERE c.%I >= %L AND c.%I < %L'
			'    AND NOT EXISTS (SELECT 1 FROM %s d'
			'                     WHERE d.%I >= %L AND d.%I < %L AND %s)'
			' UNION ALL '
			' SELECT %s FROM ('
			'   SELECT DISTINCT ON (%s) * FROM %s'
			'    WHERE %I >= %L AND %I < %L'
			'    ORDER BY %s, _ts_seq DESC) l'
			'  WHERE NOT l._ts_deleted',
			alias_cols, t.cold_table::text,
			t.time_column, p.part_start, t.time_column, p.part_end,
			t.delta_table::text,
			t.time_column, p.part_start, t.time_column, p.part_end, key_clause,
			cols,
			timeseries.quoted_list(t.key_columns), t.delta_table::text,
			t.time_column, p.part_start, t.time_column, p.part_end,
			timeseries.quoted_list(t.key_columns));

		PERFORM timeseries.copy_range(parent, p.part_start, p.part_end,
									  'pg_temp._ts_repair'::regclass);

		EXECUTE 'DROP TABLE pg_temp._ts_repair';

		EXECUTE format('DELETE FROM %s WHERE %I >= %L AND %I < %L',
					   t.delta_table::text, t.time_column, p.part_start,
					   t.time_column, p.part_end);

		UPDATE timeseries.partitions
		   SET state = 'cold_clean', synced_at = now()
		 WHERE partitions.parent = repair.parent AND partitions.part_start = p.part_start;

		repaired := repaired + 1;
	END LOOP;

	RETURN repaired;
END;
$$;
COMMENT ON FUNCTION timeseries.repair(regclass, timestamptz)
	IS 'Rematerialise mutated cold partitions from Iceberg + delta.';

/*
 * Drop cold data older than cold_retention, on partition boundaries so the
 * removal stays metadata-only. Never touches data at or above the boundary:
 * a cold_retention shorter than hot_retention simply has no effect.
 */
CREATE FUNCTION timeseries.apply_retention(parent regclass)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
	t		record;
	cutoff	timestamptz;
	dropped	int;
BEGIN
	PERFORM timeseries.check_owner(parent);

	SELECT * INTO t FROM timeseries.tables WHERE tables.parent = apply_retention.parent;
	IF NOT FOUND THEN
		RAISE EXCEPTION '% is not a pg_lake_timeseries table', parent::text;
	END IF;

	IF t.cold_retention IS NULL THEN
		RETURN 0;
	END IF;

	cutoff := least(timeseries.partition_start(now() - t.cold_retention,
											   t.partition_interval),
					t.boundary);

	EXECUTE format('DELETE FROM %s WHERE %I < %L',
				   t.cold_table::text, t.time_column, cutoff);
	EXECUTE format('DELETE FROM %s WHERE %I < %L',
				   t.delta_table::text, t.time_column, cutoff);

	DELETE FROM timeseries.partitions p
	 WHERE p.parent = apply_retention.parent AND p.part_end <= cutoff;

	GET DIAGNOSTICS dropped = ROW_COUNT;

	RETURN dropped;
END;
$$;
COMMENT ON FUNCTION timeseries.apply_retention(regclass)
	IS 'Expire cold partitions beyond cold_retention.';

/*
 * One maintenance pass for one table, in the order the state machine requires:
 * repair before seal (so the boundary never advances over a dirty partition
 * whose delta rows would then sit above it), and retention last.
 */
CREATE FUNCTION timeseries.maintain(parent regclass)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
	PERFORM timeseries.add_partitions(parent);
	PERFORM timeseries.repair(parent);
	PERFORM timeseries.sync(parent);
	PERFORM timeseries.seal(parent);
	PERFORM timeseries.apply_retention(parent);
END;
$$;
COMMENT ON FUNCTION timeseries.maintain(regclass)
	IS 'Run one maintenance pass: extend, repair, sync, seal, expire.';

-- ---------------------------------------------------------------------------
-- Maintenance worker
-- ---------------------------------------------------------------------------

CREATE FUNCTION timeseries.maintenance_worker(internal)
RETURNS internal
LANGUAGE c
AS 'MODULE_PATHNAME', $function$pg_lake_timeseries_maintenance_worker$function$;
COMMENT ON FUNCTION timeseries.maintenance_worker(internal)
	IS 'Entry point of the pg_lake_timeseries maintenance worker.';

SELECT extension_base.register_worker('pg_lake_timeseries maintenance worker',
									  'timeseries.maintenance_worker');

-- ---------------------------------------------------------------------------
-- Privileges
-- ---------------------------------------------------------------------------

/*
 * The API functions run as the calling user: the tier relations they create are
 * then owned by that user, and PostgreSQL's own privilege checks decide what a
 * caller may convert, alter or drop. What that leaves is the extension catalogs,
 * which every table owner has to write. Rather than funnel those writes through
 * SECURITY DEFINER functions -- which cannot see who called them, and would have
 * to trust their arguments -- the catalogs are writable by all and constrained by
 * row-level security: a row belongs to the owner of its `parent` relation.
 *
 * The WITH CHECK half is the part that matters. timeseries.maintain() is called
 * by a superuser background worker and does whatever the catalog row says,
 * including dropping heap partitions and overwriting the Iceberg table; requiring
 * that the inserting user owns every relation a row names is what stops one user
 * from aiming that worker at another user's tables.
 */
ALTER TABLE timeseries.tables ENABLE ROW LEVEL SECURITY;
ALTER TABLE timeseries.partitions ENABLE ROW LEVEL SECURITY;

CREATE POLICY table_owner ON timeseries.tables
	USING (timeseries.is_owner(parent))
	WITH CHECK (timeseries.is_owner(parent)
				AND timeseries.is_owner(hot_table)
				AND timeseries.is_owner(cold_table)
				AND timeseries.is_owner(cold_scan)
				AND timeseries.is_owner(delta_table)
				AND timeseries.is_owner(seq_sequence));

CREATE POLICY partition_owner ON timeseries.partitions
	USING (timeseries.is_owner(parent))
	WITH CHECK (timeseries.is_owner(parent)
				AND (hot_partition IS NULL OR timeseries.is_owner(hot_partition)));

GRANT SELECT, INSERT, UPDATE, DELETE ON timeseries.tables, timeseries.partitions TO public;

/* only the base-worker framework calls this */
REVOKE ALL ON FUNCTION timeseries.maintenance_worker(internal) FROM public;

/*
 * mark_dirty() writes the partition catalog on behalf of a writer who need not
 * own the table, so it is reached only through route_write(), which is SECURITY
 * DEFINER and passes it a parent it has already resolved from TG_RELID.
 */
REVOKE ALL ON FUNCTION timeseries.mark_dirty(regclass, timestamptz) FROM public;
