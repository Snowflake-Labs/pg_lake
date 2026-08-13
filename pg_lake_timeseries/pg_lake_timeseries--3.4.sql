/*
 * pg_lake_timeseries--3.4.sql
 *
 * Your own range-partitioned table is the hot tier; a pg_lake Iceberg table is
 * the cold one; a planner hook puts them back together (DESIGN.md section 7).
 *
 *   - the relation stays exactly the table you created, with your indexes,
 *     constraints and native tuple routing. Nothing is renamed, wrapped or
 *     replaced, and writes go straight to it;
 *   - a stored authority boundary B splits the two tiers: PostgreSQL owns
 *     time_column >= B, Iceberg owns time_column < B. Both branches carry an
 *     explicit bound, so every row is returned exactly once;
 *   - B is advanced only by seal(), which has proven in the same transaction
 *     that the range is in Iceberg before dropping the heap partition. The
 *     Iceberg copy of the still-hot window is therefore allowed to lag: nobody
 *     reads it through PostgreSQL, and external Iceberg readers get a
 *     consistent, if slightly stale, view of it.
 *
 * There is no delta and no merge-on-read. A range is read from one tier or the
 * other, never both, so a mutation of already-sealed data is a plain error
 * rather than an overlay -- the ranges below B have no heap partition to hold it.
 *
 * The metadata below is written only through the C functions in src/registry.c,
 * which check that the caller owns the relation and then switch to the extension
 * owner. The tables themselves grant nothing to anybody, and carry no row-level
 * security: RLS on an extension configuration table makes pg_dump fail (it
 * refuses to dump a table whose policies could silently filter rows), which is
 * exactly the data this extension needs dumped.
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
 * One row per tiered table. `relation` is the user's own table: an ordinary
 * heap, RANGE-partitioned on time_column. That OID is the mark the planner hook
 * looks for, which is why it is the primary key.
 *
 * Read from C without SPI (src/metadata.c): a planner hook cannot run SPI,
 * because that would re-enter the planner.
 */
CREATE TABLE timeseries.tables (
	relation			regclass PRIMARY KEY,
	-- pg_lake Iceberg table with the same columns in the same order,
	-- authoritative for time_column < boundary
	cold_table			regclass	NOT NULL,
	-- time (partitioning) column of relation, a NOT NULL timestamp
	time_column			name		NOT NULL,
	-- fixed-length partition granularity of relation (hour/day/week)
	partition_interval	interval	NOT NULL,
	-- authority boundary B. Starts at -infinity (Iceberg owns nothing) unless
	-- the cold tier was pre-loaded with history; advanced only by seal().
	boundary			timestamptz	NOT NULL,
	-- how much time stays authoritative in PostgreSQL before a partition seals
	hot_retention		interval	NOT NULL,
	-- how much history the Iceberg tier keeps, NULL for unlimited
	cold_retention		interval,
	-- how many partition intervals ahead of now() partitions are pre-created
	precreate_ahead		int			NOT NULL DEFAULT 7
);

/*
 * What Iceberg holds a copy of, one row per partition-aligned range.
 *
 * `synced_at` is when the copy was last refreshed and `sealed_at` is when
 * Iceberg became authoritative for the range -- which is to say, when the heap
 * partition was dropped and B moved past part_end.
 *
 * The planner never reads this table. It only needs B, and B is a single value
 * on the row above; per-range state is a maintenance concern. That is the whole
 * reason the read path can be cached per backend and invalidated rarely.
 */
CREATE TABLE timeseries.partitions (
	relation	regclass	NOT NULL
		REFERENCES timeseries.tables(relation) ON DELETE CASCADE,
	part_start	timestamptz	NOT NULL,
	part_end	timestamptz	NOT NULL,
	synced_at	timestamptz	NOT NULL,
	sealed_at	timestamptz,
	PRIMARY KEY (relation, part_start),
	CHECK (part_end > part_start)
);

SELECT pg_catalog.pg_extension_config_dump('timeseries.tables', '');
SELECT pg_catalog.pg_extension_config_dump('timeseries.partitions', '');

-- ---------------------------------------------------------------------------
-- Cache invalidation
-- ---------------------------------------------------------------------------

/*
 * Drop what every backend cached about the registry.
 *
 * DML on timeseries.tables raises no invalidation by itself, so this has to be
 * explicit. It hangs off a statement trigger rather than off the C writers so
 * that a row restored by pg_restore, or edited by a superuser, is picked up too.
 *
 * A trigger function rather than a plain function called from plpgsql, because
 * EXECUTE on a trigger function is checked when the trigger is created and not
 * when it fires: the invalidation therefore works for whoever writes the catalog,
 * including a pg_restore running as someone with no rights to this schema at all.
 */
CREATE FUNCTION timeseries.invalidate_cache_trigger()
RETURNS trigger
AS 'MODULE_PATHNAME', 'timeseries_invalidate_cache_trigger'
LANGUAGE C;

CREATE TRIGGER invalidate_cache
AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON timeseries.tables
FOR EACH STATEMENT EXECUTE FUNCTION timeseries.invalidate_cache_trigger();

-- ---------------------------------------------------------------------------
-- Metadata reads
-- ---------------------------------------------------------------------------

/*
 * These read the catalog with systable_beginscan, which applies no ACL check --
 * deliberately, and the same way the planner hook does it. Whether a relation is
 * tiered, and where its boundary sits, is a property of the relation that every
 * backend has to agree about, whoever is running the query. It stays a privilege
 * check rather than a visibility one: a user who may not read the Iceberg tier
 * gets a permission error on it, not a silently different answer.
 */

/* Whether a relation is registered, answered from the cache the planner uses. */
CREATE FUNCTION timeseries.is_tiered(relation regclass)
RETURNS boolean
AS 'MODULE_PATHNAME', 'timeseries_is_tiered'
LANGUAGE C STRICT STABLE;
COMMENT ON FUNCTION timeseries.is_tiered(regclass)
	IS 'Whether a relation is registered as a tiered time-series table.';

/* The registration of one tiered table, all-NULL if it is not registered. */
CREATE FUNCTION timeseries.tiered_table(relation regclass,
										OUT cold_table regclass,
										OUT time_column name,
										OUT partition_interval interval,
										OUT boundary timestamptz,
										OUT hot_retention interval,
										OUT cold_retention interval,
										OUT precreate_ahead int)
RETURNS record
AS 'MODULE_PATHNAME', 'timeseries_tiered_table'
LANGUAGE C STRICT STABLE;
COMMENT ON FUNCTION timeseries.tiered_table(regclass)
	IS 'The registration of a tiered table: tiers, boundary and retention.';

/* Every registered table. */
CREATE FUNCTION timeseries.tiered_tables(OUT relation regclass,
										 OUT cold_table regclass,
										 OUT time_column name,
										 OUT partition_interval interval,
										 OUT boundary timestamptz,
										 OUT hot_retention interval,
										 OUT cold_retention interval,
										 OUT precreate_ahead int)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'timeseries_tiered_tables'
LANGUAGE C STRICT STABLE;
COMMENT ON FUNCTION timeseries.tiered_tables()
	IS 'All tiered time-series tables.';

/* What Iceberg holds a copy of, and which of those ranges it owns. */
CREATE FUNCTION timeseries.synced_ranges(relation regclass,
										 OUT part_start timestamptz,
										 OUT part_end timestamptz,
										 OUT synced_at timestamptz,
										 OUT sealed_at timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'timeseries_synced_ranges'
LANGUAGE C STRICT STABLE;
COMMENT ON FUNCTION timeseries.synced_ranges(regclass)
	IS 'Ranges of a tiered table that are present in its Iceberg tier.';

/*
 * The partitions of a range-partitioned table and the range each covers, read
 * from relpartbound rather than parsed back out of pg_get_expr(). MINVALUE and
 * MAXVALUE come back as NULL; a DEFAULT partition is not reported, because it
 * covers no definite range.
 *
 * This is what lets the extension manage a table whose partitions somebody else
 * created: the heap itself says which ranges exist, so nothing has to be
 * mirrored into timeseries.partitions to be trusted.
 */
CREATE FUNCTION timeseries.heap_ranges(relation regclass,
									   OUT partition regclass,
									   OUT part_start timestamptz,
									   OUT part_end timestamptz)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'timeseries_heap_ranges'
LANGUAGE C STRICT STABLE;
COMMENT ON FUNCTION timeseries.heap_ranges(regclass)
	IS 'The partitions of a range-partitioned table and the range each covers.';

-- ---------------------------------------------------------------------------
-- Metadata writes
-- ---------------------------------------------------------------------------

/*
 * Each of these checks that the calling user owns `relation` and then performs
 * the write as the extension owner (SPI_START_EXTENSION_OWNER). The check has to
 * happen in C: inside a SECURITY DEFINER plpgsql function current_user is the
 * definer, and there is no way to see who called it, so plpgsql could not
 * authorize this at all.
 *
 * Registering a table is not among them, because it is not something to call:
 * CREATE TABLE ... USING timeseries registers, from C, the tables it has just
 * created (src/ddl.c). What is left here is what maintenance records as it runs,
 * and each of these verifies ownership of the relation whose metadata it changes.
 * That is what keeps one user from aiming the superuser maintenance worker, which
 * will happily drop partitions and overwrite an Iceberg table on the strength of
 * a catalog row, at another user's tables.
 */

/* Record that Iceberg now holds a copy of a range. */
CREATE FUNCTION timeseries.record_sync(relation regclass,
									   part_start timestamptz,
									   part_end timestamptz)
RETURNS void
AS 'MODULE_PATHNAME', 'timeseries_record_sync'
LANGUAGE C STRICT;

/*
 * Record that Iceberg is now authoritative for a range, and move the boundary.
 * One function because it is one fact: the boundary may only advance over a
 * range that this same transaction sealed.
 */
CREATE FUNCTION timeseries.record_seal(relation regclass,
									   part_start timestamptz,
									   part_end timestamptz,
									   new_boundary timestamptz)
RETURNS void
AS 'MODULE_PATHNAME', 'timeseries_record_seal'
LANGUAGE C STRICT;

/* Forget ranges that retention removed. Returns how many rows went. */
CREATE FUNCTION timeseries.forget_ranges(relation regclass, cutoff timestamptz)
RETURNS int
AS 'MODULE_PATHNAME', 'timeseries_forget_ranges'
LANGUAGE C STRICT;

/*
 * Unregister tables whose relations were dropped.
 *
 * A regclass column is a plain OID: it carries no dependency, so DROP TABLE
 * would otherwise leave a registration naming a relation that no longer exists
 * -- and OIDs are reused, so a later table could inherit someone else's mark.
 *
 * Unlike its neighbours this checks no ownership, and needs none: a row it can
 * delete names a relation that is not in pg_class, so it describes no table and
 * nobody owns it.
 */
CREATE FUNCTION timeseries.forget_dropped()
RETURNS int
AS 'MODULE_PATHNAME', 'timeseries_forget_dropped'
LANGUAGE C;
COMMENT ON FUNCTION timeseries.forget_dropped()
	IS 'Unregister tiered tables whose relations no longer exist.';

/*
 * This is a sweep for registrations naming a relation that is gone, rather than
 * a lookup of the objects the command dropped, because
 * pg_event_trigger_dropped_objects() does not report the Iceberg tier: pg_lake
 * drops its own tables by calling RemoveRelations() directly
 * (pg_lake_table/src/ddl/drop_table.c), which is outside the path that collects
 * dropped objects, so no sql_drop event fires for them at all. ddl_command_end
 * does fire, for both tiers.
 */
CREATE FUNCTION timeseries.handle_drop()
RETURNS event_trigger
LANGUAGE plpgsql AS $$
BEGIN
	/* every DDL statement in the database reaches this */
	IF tg_tag LIKE 'DROP %' THEN
		PERFORM timeseries.forget_dropped();
	END IF;
END;
$$;

CREATE EVENT TRIGGER timeseries_handle_drop ON ddl_command_end
	EXECUTE FUNCTION timeseries.handle_drop();

-- ---------------------------------------------------------------------------
-- Helpers
-- ---------------------------------------------------------------------------

/*
 * Floor a timestamp to a partition boundary. Only fixed-length intervals are
 * supported: month/year granularities would need date_trunc and are rejected
 * when the table is created.
 */
CREATE FUNCTION timeseries.partition_start(ts timestamptz, part_interval interval)
RETURNS timestamptz
LANGUAGE sql IMMUTABLE STRICT AS $$
	SELECT to_timestamp(floor(extract(epoch FROM ts) / extract(epoch FROM part_interval))
						* extract(epoch FROM part_interval))
$$;
COMMENT ON FUNCTION timeseries.partition_start(timestamptz, interval)
	IS 'Floor a timestamp to a fixed-length partition boundary.';

/* Whether the current user owns (or is a member of the role owning) a relation. */
CREATE FUNCTION timeseries.is_owner(rel regclass)
RETURNS boolean
LANGUAGE sql STABLE STRICT AS $$
	SELECT pg_catalog.pg_has_role(current_user, c.relowner, 'USAGE')
	  FROM pg_catalog.pg_class c WHERE c.oid = rel
$$;

/*
 * Raise unless the caller owns the table. The C writers check this too, and
 * theirs is the one that counts; this one is here so that a function which
 * touches user data before its first metadata write fails before it starts.
 */
CREATE FUNCTION timeseries.check_owner(rel regclass)
RETURNS void
LANGUAGE plpgsql STRICT AS $$
BEGIN
	IF NOT coalesce(timeseries.is_owner(rel), false) THEN
		RAISE EXCEPTION 'must be owner of relation %', rel::text
			USING ERRCODE = 'insufficient_privilege';
	END IF;
END;
$$;

-- ---------------------------------------------------------------------------
-- The timeseries access method
-- ---------------------------------------------------------------------------

/*
 * CREATE TABLE ... USING timeseries is how a tiered table is made, and this is
 * the access method that names.
 *
 * Nothing ever routes through the handler. The utility hook in src/ddl.c
 * intercepts the CREATE TABLE before PostgreSQL looks the access method up, and
 * what it creates is an ordinary partitioned heap plus an Iceberg table -- there
 * is no relation whose relam is this one. The access method exists so that the
 * name resolves at parse analysis, so that USING timeseries is a syntax error
 * when the extension is not installed rather than a table nobody manages, and so
 * that pg_am lists it.
 *
 * The handler therefore only ever runs if the interception did not happen, which
 * means the module is not in shared_preload_libraries. Raising is the right
 * answer to that, and a far better one than a table that looks tiered and is not.
 */
CREATE FUNCTION timeseries.am_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME', 'timeseries_am_handler'
LANGUAGE C;
COMMENT ON FUNCTION timeseries.am_handler(internal)
	IS 'Placeholder handler for the timeseries access method; CREATE TABLE ... USING timeseries is handled before it is reached.';

CREATE ACCESS METHOD timeseries TYPE TABLE HANDLER timeseries.am_handler;
COMMENT ON ACCESS METHOD timeseries
	IS 'Time-series table whose history is tiered to Apache Iceberg.';

-- ---------------------------------------------------------------------------
-- Maintenance
-- ---------------------------------------------------------------------------

/*
 * Extend the partition frontier so the insert path never has to run DDL.
 *
 * Partitions are created ahead of now() (precreate_ahead intervals by default).
 * There is no DEFAULT partition: a timestamp beyond the frontier is a bug in
 * maintenance rather than something to absorb silently, and a DEFAULT partition
 * would block attaching the next range. A timestamp *below* the frontier has no
 * partition either, which is what makes a write below the boundary an error
 * instead of a row Iceberg will never see.
 */
CREATE FUNCTION timeseries.add_partitions(relation regclass,
										  upto timestamptz DEFAULT NULL)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
	t			record;
	nsp			name;
	rel			name;
	part_start	timestamptz;
	part_end	timestamptz;
	part_name	text;
	created		int := 0;
BEGIN
	PERFORM timeseries.check_owner(relation);

	SELECT * INTO t FROM timeseries.tiered_table(add_partitions.relation);
	IF t.cold_table IS NULL THEN
		RAISE EXCEPTION '% is not a tiered table', relation::text;
	END IF;

	/* an unbounded partition already covers everything there is to add */
	IF EXISTS (SELECT 1 FROM timeseries.heap_ranges(add_partitions.relation) h
				WHERE h.part_end IS NULL) THEN
		RETURN 0;
	END IF;

	upto := coalesce(upto, now() + t.precreate_ahead * t.partition_interval);

	/*
	 * Start at the frontier the heap itself reports. For a table with no
	 * partitions yet, start at the beginning of the hot window, or at the
	 * boundary when the cold tier was pre-loaded past it -- GREATEST ignores the
	 * NULL that an -infinity boundary produces.
	 */
	SELECT max(h.part_end) INTO part_start
	  FROM timeseries.heap_ranges(add_partitions.relation) h;

	part_start := coalesce(
		part_start,
		greatest(timeseries.partition_start(now() - t.hot_retention,
											t.partition_interval),
				 CASE WHEN t.boundary = '-infinity' THEN NULL
					  ELSE timeseries.partition_start(t.boundary,
													  t.partition_interval) END));

	SELECT n.nspname, c.relname INTO nsp, rel
	  FROM pg_catalog.pg_class c
	  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	 WHERE c.oid = add_partitions.relation;

	WHILE part_start <= upto LOOP
		part_end := part_start + t.partition_interval;
		part_name := format('%I.%I', nsp,
							left(rel, 40) || '_' ||
							to_char(part_start AT TIME ZONE 'UTC',
									'YYYYMMDD"t"HH24MI'));

		EXECUTE format('CREATE TABLE %s PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
					   part_name, relation::text, part_start, part_end);

		part_start := part_end;
		created := created + 1;
	END LOOP;

	RETURN created;
END;
$$;
COMMENT ON FUNCTION timeseries.add_partitions(regclass, timestamptz)
	IS 'Extend the partition frontier of a tiered table up to a point in time.';

/*
 * Refresh the Iceberg copy of partitions that are entirely in the past.
 *
 * These rows are still authoritative in PostgreSQL -- the boundary does not move
 * here -- so the copy is invisible to queries through the relation and can be
 * redone at any time. What it buys is that an external Iceberg reader sees
 * everything up to the last completed partition instead of only sealed history.
 *
 * A partition is copied once, after it stops receiving in-order writes. A later
 * mutation of a still-hot partition is picked up by the re-copy in seal(); until
 * then an external reader sees the older copy. Pass `only_start` to force one.
 */
CREATE FUNCTION timeseries.sync(relation regclass,
								only_start timestamptz DEFAULT NULL)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
	t		record;
	r		record;
	synced	int := 0;
BEGIN
	PERFORM timeseries.check_owner(relation);

	SELECT * INTO t FROM timeseries.tiered_table(sync.relation);
	IF t.cold_table IS NULL THEN
		RAISE EXCEPTION '% is not a tiered table', relation::text;
	END IF;

	FOR r IN
		SELECT h.partition, h.part_start, h.part_end
		  FROM timeseries.heap_ranges(sync.relation) h
		  LEFT JOIN timeseries.synced_ranges(sync.relation) s
				 ON s.part_start = h.part_start
		 WHERE h.part_start IS NOT NULL AND h.part_end IS NOT NULL
		   AND h.part_end <= now()
		   AND (only_start IS NULL OR h.part_start = only_start)
		   AND (only_start IS NOT NULL
				OR s.synced_at IS NULL OR s.synced_at < h.part_end)
		 ORDER BY h.part_start
	LOOP
		/*
		 * Overwrite the range rather than append to it: the copy has to be
		 * repeatable, and the predicate prunes the cold side to the one range.
		 *
		 * The rows are read from the partition and not from the relation, which
		 * the planner would expand into both tiers -- reading the table being
		 * written, to add rows that are by definition not in this range.
		 */
		EXECUTE format('DELETE FROM %s WHERE %I >= %L AND %I < %L',
					   t.cold_table::text, t.time_column, r.part_start,
					   t.time_column, r.part_end);
		EXECUTE format('INSERT INTO %s SELECT * FROM %s',
					   t.cold_table::text, r.partition::text);

		PERFORM timeseries.record_sync(sync.relation, r.part_start, r.part_end);

		synced := synced + 1;
	END LOOP;

	RETURN synced;
END;
$$;
COMMENT ON FUNCTION timeseries.sync(regclass, timestamptz)
	IS 'Refresh the (non-authoritative) Iceberg copy of past partitions.';

/*
 * Hand partitions that aged out of the hot window over to Iceberg and advance the
 * authority boundary.
 *
 * Sealing is the only operation that moves the boundary. Copy, DROP TABLE and the
 * new boundary are one transaction, so a crash or error anywhere rolls back all
 * three: the boundary never advances past data that is not in the cold tier.
 *
 * The copy is not read back to verify it, because it cannot be -- the Iceberg
 * snapshot this transaction wrote does not exist until it commits. Atomicity is
 * the argument, not verification.
 *
 * Sealing proceeds contiguously upward from the boundary. A gap would mean
 * claiming Iceberg authority for a range that was never sealed, so a gap stops
 * the pass rather than being skipped over.
 */
CREATE FUNCTION timeseries.seal(relation regclass, upto timestamptz DEFAULT NULL)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
	t				record;
	r				record;
	seal_upto		timestamptz;
	new_boundary	timestamptz;
	sealed			int := 0;
BEGIN
	PERFORM timeseries.check_owner(relation);

	SELECT * INTO t FROM timeseries.tiered_table(seal.relation);
	IF t.cold_table IS NULL THEN
		RAISE EXCEPTION '% is not a tiered table', relation::text;
	END IF;

	/*
	 * Partitions that end before the hot window may be sealed. `upto` moves that
	 * point explicitly, which is how a table is shrunk on demand instead of
	 * waiting for the retention interval to elapse.
	 */
	seal_upto := timeseries.partition_start(coalesce(upto, now() - t.hot_retention),
											t.partition_interval);
	new_boundary := t.boundary;

	FOR r IN
		SELECT h.partition, h.part_start, h.part_end
		  FROM timeseries.heap_ranges(seal.relation) h
		 WHERE h.part_start IS NOT NULL AND h.part_end IS NOT NULL
		   AND h.part_end <= seal_upto
		 ORDER BY h.part_start
	LOOP
		IF new_boundary <> '-infinity' AND r.part_start <> new_boundary THEN
			RAISE WARNING 'gap in the partitions of % at %, stopping seal',
						  relation::text, new_boundary;
			EXIT;
		END IF;

		EXECUTE format('DELETE FROM %s WHERE %I >= %L AND %I < %L',
					   t.cold_table::text, t.time_column, r.part_start,
					   t.time_column, r.part_end);
		EXECUTE format('INSERT INTO %s SELECT * FROM %s',
					   t.cold_table::text, r.partition::text);

		EXECUTE format('DROP TABLE %s', r.partition::text);

		new_boundary := r.part_end;

		PERFORM timeseries.record_seal(seal.relation, r.part_start, r.part_end,
									   new_boundary);

		sealed := sealed + 1;
	END LOOP;

	RETURN sealed;
END;
$$;
COMMENT ON FUNCTION timeseries.seal(regclass, timestamptz)
	IS 'Move aged-out partitions to Iceberg and advance the authority boundary.';

/*
 * Drop cold data older than cold_retention, on partition boundaries so the
 * removal stays metadata-only in Iceberg. Never touches data at or above the
 * boundary: a cold_retention shorter than hot_retention simply has no effect.
 */
CREATE FUNCTION timeseries.apply_retention(relation regclass)
RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
	t		record;
	cutoff	timestamptz;
BEGIN
	PERFORM timeseries.check_owner(relation);

	SELECT * INTO t FROM timeseries.tiered_table(apply_retention.relation);
	IF t.cold_table IS NULL THEN
		RAISE EXCEPTION '% is not a tiered table', relation::text;
	END IF;

	IF t.cold_retention IS NULL THEN
		RETURN 0;
	END IF;

	cutoff := least(timeseries.partition_start(now() - t.cold_retention,
											   t.partition_interval),
					t.boundary);

	EXECUTE format('DELETE FROM %s WHERE %I < %L',
				   t.cold_table::text, t.time_column, cutoff);

	RETURN timeseries.forget_ranges(apply_retention.relation, cutoff);
END;
$$;
COMMENT ON FUNCTION timeseries.apply_retention(regclass)
	IS 'Expire cold data beyond cold_retention.';

/*
 * One maintenance pass for one table: extend the frontier, refresh the lagging
 * copy, seal what aged out, then expire. Sealing after syncing means a partition
 * that was already copied is copied once more before it is dropped, which is what
 * makes the copy complete rather than merely recent.
 */
CREATE FUNCTION timeseries.maintain(relation regclass)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
	PERFORM timeseries.add_partitions(relation);
	PERFORM timeseries.sync(relation);
	PERFORM timeseries.seal(relation);
	PERFORM timeseries.apply_retention(relation);
END;
$$;
COMMENT ON FUNCTION timeseries.maintain(regclass)
	IS 'Run one maintenance pass: extend, sync, seal, expire.';

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
 * The catalogs grant nothing: they are written only through the C functions
 * above, which check ownership and then switch to the extension owner, and read
 * only through the C readers, which bypass ACLs the same way the planner does.
 *
 * pg_monitor gets SELECT so that monitoring can see what is registered and how
 * far each boundary has moved without being able to change any of it. Note that
 * this makes a plain pg_dump by a user who is neither superuser nor a member of
 * pg_monitor (or pg_read_all_data) fail on the two configuration tables. That is
 * the trade for not using row-level security, which would make pg_dump fail
 * outright -- it refuses to dump a table whose policies might silently filter the
 * rows it is copying.
 */
GRANT SELECT ON timeseries.tables, timeseries.partitions TO pg_monitor;

/* only the base-worker framework calls this */
REVOKE ALL ON FUNCTION timeseries.maintenance_worker(internal) FROM public;

/* only the statement trigger on timeseries.tables calls this */
REVOKE ALL ON FUNCTION timeseries.invalidate_cache_trigger() FROM public;
