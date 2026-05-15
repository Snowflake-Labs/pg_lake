-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_lake_polaris" to load this file. \quit


/*
 * Schema and bookkeeping tables. The schema is created by the .control
 * file's `schema = lake_polaris`.
 */

CREATE TABLE lake_polaris.catalog_mapping (
    pg_database name PRIMARY KEY,
    polaris_catalog_name text NOT NULL,
    polaris_catalog_id bigint NOT NULL,
    polaris_realm_id text NOT NULL,
    enabled boolean NOT NULL DEFAULT true
);

CREATE TABLE lake_polaris.entity_link (
    pg_table_oid regclass PRIMARY KEY,
    polaris_entity_id bigint NOT NULL UNIQUE,
    polaris_namespace_id bigint NOT NULL,
    last_seen_metadata_location text
);


/*
 * Suppress flag wrappers (C functions). These are read/written by the SQL
 * trigger functions to break the round-trip loop between the outbound
 * trigger on lake_iceberg.tables_internal and the inbound trigger on
 * polaris_schema.entities.
 */

CREATE FUNCTION lake_polaris.is_suppressed()
    RETURNS boolean
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', 'pg_lake_polaris_is_suppressed';

CREATE FUNCTION lake_polaris.set_suppressed(new_value boolean)
    RETURNS boolean
    LANGUAGE C VOLATILE STRICT
    AS 'MODULE_PATHNAME', 'pg_lake_polaris_set_suppressed';


/*
 * Type-code constants. Mirror Apache Polaris's PolarisEntityType enum.
 *   CATALOG     = 4
 *   NAMESPACE   = 6
 *   TABLE_LIKE  = 7
 *
 * Sub-type for an Iceberg table is 2 (PolarisEntitySubType.ICEBERG_TABLE).
 */
CREATE FUNCTION lake_polaris.type_code_catalog()    RETURNS int LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT 4 $$;
CREATE FUNCTION lake_polaris.type_code_namespace()  RETURNS int LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT 6 $$;
CREATE FUNCTION lake_polaris.type_code_table_like() RETURNS int LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT 7 $$;
CREATE FUNCTION lake_polaris.sub_type_iceberg()     RETURNS int LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$ SELECT 2 $$;


/*
 * Helper: derive the storage prefix from a metadata location URI.
 *
 * Iceberg metadata files live at <prefix>/metadata/<n>-<uuid>.metadata.json.
 * Polaris stores the prefix (without the /metadata/... suffix) in the
 * `location` field of the table entity.
 */
CREATE FUNCTION lake_polaris.location_from_metadata(metadata_location text)
    RETURNS text
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    AS $$
        SELECT regexp_replace(metadata_location,
                              '(.*?/)(metadata/[^/]*\.metadata\.json)$',
                              '\1')
    $$;


/*
 * outbound_get_or_create_namespace ensures a namespace entity exists in
 * polaris_schema.entities for the given Postgres schema name, parented at
 * the configured catalog. Returns the namespace's polaris id.
 *
 * The namespace's `location` property mirrors the table's location prefix
 * so Polaris can serve it for `loadNamespaceMetadata`. We use the table's
 * prefix because there's no separate per-schema location concept on the
 * pg_lake side.
 */
CREATE FUNCTION lake_polaris.outbound_get_or_create_namespace(
    p_catalog_id bigint,
    p_realm_id text,
    p_namespace_name text,
    p_location text)
    RETURNS bigint
    LANGUAGE plpgsql
    AS $function$
DECLARE
    v_namespace_id bigint;
    v_now bigint := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
BEGIN
    SELECT id INTO v_namespace_id
    FROM polaris_schema.entities
    WHERE realm_id = p_realm_id
      AND catalog_id = p_catalog_id
      AND parent_id = p_catalog_id
      AND type_code = lake_polaris.type_code_namespace()
      AND name = p_namespace_name
    FOR UPDATE;

    IF FOUND THEN
        RETURN v_namespace_id;
    END IF;

    /*
     * v0.1 limitation: random ids may collide with Polaris's own allocator
     * if Polaris itself is allocating ids concurrently. The unique
     * constraint on (realm_id, id) will catch collisions; we don't retry
     * here, so registration must be done while Polaris is quiescent.
     */
    v_namespace_id := (random() * 9223372036854775807)::bigint;

    INSERT INTO polaris_schema.entities (
        realm_id, catalog_id, id, parent_id, name, entity_version,
        type_code, sub_type_code, create_timestamp, drop_timestamp,
        purge_timestamp, to_purge_timestamp, last_update_timestamp,
        properties, internal_properties, grant_records_version)
    VALUES (
        p_realm_id, p_catalog_id, v_namespace_id, p_catalog_id,
        p_namespace_name, 1, lake_polaris.type_code_namespace(), 0,
        v_now, 0, 0, 0, v_now,
        jsonb_build_object('location', p_location)::text,
        '{}'::text, 1);

    RETURN v_namespace_id;
END;
$function$;


/*
 * outbound_sync writes (or refreshes) a Polaris entity row for the given
 * pg_lake table. Idempotent: safe to call on every INSERT/UPDATE of a
 * tables_internal row.
 *
 * Bumps entity_version on update so Polaris's optimistic-concurrency
 * checks notice the change.
 */
CREATE FUNCTION lake_polaris.outbound_sync(p_table_oid regclass)
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
DECLARE
    v_mapping lake_polaris.catalog_mapping%ROWTYPE;
    v_namespace_name text;
    v_table_name text;
    v_metadata_location text;
    v_location text;
    v_namespace_id bigint;
    v_table_id bigint;
    v_now bigint := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
BEGIN
    SELECT * INTO v_mapping
    FROM lake_polaris.catalog_mapping
    WHERE pg_database = current_database();

    IF NOT FOUND OR NOT v_mapping.enabled THEN
        RETURN;
    END IF;

    SELECT n.nspname, c.relname
      INTO v_namespace_name, v_table_name
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_table_oid;

    IF v_namespace_name IS NULL THEN
        RAISE EXCEPTION 'pg_lake_polaris: relation % no longer exists',
            p_table_oid;
    END IF;

    SELECT metadata_location INTO v_metadata_location
    FROM lake_iceberg.tables_internal
    WHERE table_name = p_table_oid;

    /*
     * If metadata_location is NULL the table has been created but no
     * metadata file has been written yet (CREATE TABLE without INSERT).
     * Defer mirroring until the first metadata write fires the trigger
     * again.
     */
    IF v_metadata_location IS NULL THEN
        RETURN;
    END IF;

    v_location := lake_polaris.location_from_metadata(v_metadata_location);

    v_namespace_id := lake_polaris.outbound_get_or_create_namespace(
        v_mapping.polaris_catalog_id,
        v_mapping.polaris_realm_id,
        v_namespace_name,
        v_location);

    /* upsert the table entity */
    SELECT polaris_entity_id INTO v_table_id
    FROM lake_polaris.entity_link
    WHERE pg_table_oid = p_table_oid;

    IF v_table_id IS NULL THEN
        v_table_id := (random() * 9223372036854775807)::bigint;

        INSERT INTO polaris_schema.entities (
            realm_id, catalog_id, id, parent_id, name, entity_version,
            type_code, sub_type_code, create_timestamp, drop_timestamp,
            purge_timestamp, to_purge_timestamp, last_update_timestamp,
            properties, internal_properties, grant_records_version)
        VALUES (
            v_mapping.polaris_realm_id, v_mapping.polaris_catalog_id,
            v_table_id, v_namespace_id, v_table_name, 1,
            lake_polaris.type_code_table_like(),
            lake_polaris.sub_type_iceberg(),
            v_now, 0, 0, 0, v_now,
            jsonb_build_object('location', v_location)::text,
            jsonb_build_object('parent-namespace', v_namespace_name,
                               'metadata-location', v_metadata_location)::text,
            1);

        INSERT INTO lake_polaris.entity_link
            (pg_table_oid, polaris_entity_id, polaris_namespace_id,
             last_seen_metadata_location)
        VALUES
            (p_table_oid, v_table_id, v_namespace_id, v_metadata_location);
    ELSE
        UPDATE polaris_schema.entities
           SET entity_version = entity_version + 1,
               internal_properties = jsonb_build_object(
                    'parent-namespace', v_namespace_name,
                    'metadata-location', v_metadata_location)::text,
               properties = jsonb_build_object('location', v_location)::text,
               last_update_timestamp = v_now
         WHERE realm_id = v_mapping.polaris_realm_id
           AND id = v_table_id;

        UPDATE lake_polaris.entity_link
           SET last_seen_metadata_location = v_metadata_location
         WHERE pg_table_oid = p_table_oid;
    END IF;
END;
$function$;


/*
 * outbound_remove deletes the Polaris entity row for a pg_lake table that
 * has been dropped. The namespace entity is intentionally left in place —
 * it may have been created out-of-band or hold other tables.
 */
CREATE FUNCTION lake_polaris.outbound_remove(p_table_oid regclass)
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
DECLARE
    v_mapping lake_polaris.catalog_mapping%ROWTYPE;
    v_link lake_polaris.entity_link%ROWTYPE;
BEGIN
    SELECT * INTO v_mapping
    FROM lake_polaris.catalog_mapping
    WHERE pg_database = current_database();

    IF NOT FOUND OR NOT v_mapping.enabled THEN
        RETURN;
    END IF;

    SELECT * INTO v_link
    FROM lake_polaris.entity_link
    WHERE pg_table_oid = p_table_oid;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    DELETE FROM polaris_schema.entities
     WHERE realm_id = v_mapping.polaris_realm_id
       AND id = v_link.polaris_entity_id;

    DELETE FROM lake_polaris.entity_link
     WHERE pg_table_oid = p_table_oid;
END;
$function$;


/*
 * outbound_trigger fires AFTER each row change on
 * lake_iceberg.tables_internal. It sets the suppress flag while writing
 * to polaris_schema.entities to prevent the inbound trigger from firing
 * a sync back into pg_lake.
 *
 * Skipped entirely when the suppress flag is already set (an inbound sync
 * is in flight) or when the database has no catalog_mapping row.
 */
CREATE FUNCTION lake_polaris.outbound_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $function$
DECLARE
    v_old_suppress boolean;
BEGIN
    IF lake_polaris.is_suppressed() THEN
        RETURN NULL;
    END IF;

    v_old_suppress := lake_polaris.set_suppressed(true);

    BEGIN
        IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
            PERFORM lake_polaris.outbound_sync(NEW.table_name);
        ELSIF TG_OP = 'DELETE' THEN
            PERFORM lake_polaris.outbound_remove(OLD.table_name);
        END IF;
    EXCEPTION WHEN OTHERS THEN
        PERFORM lake_polaris.set_suppressed(v_old_suppress);
        RAISE;
    END;

    PERFORM lake_polaris.set_suppressed(v_old_suppress);
    RETURN NULL;
END;
$function$;


/*
 * The trigger lives on a table owned by another extension. It still gets
 * cleaned up by DROP EXTENSION CASCADE because it depends on
 * lake_polaris.outbound_trigger(); the user has to use CASCADE because
 * the trigger isn't owned by either extension's pg_extension row.
 */
CREATE TRIGGER outbound_trigger
AFTER INSERT OR UPDATE OR DELETE ON lake_iceberg.tables_internal
FOR EACH ROW EXECUTE FUNCTION lake_polaris.outbound_trigger();


/*
 * Inbound sync: react to changes that another Polaris client made to
 * polaris_schema.entities (e.g., a Spark or PyIceberg client committing
 * via Polaris's REST API).
 *
 * Three cases, keyed off the trigger event:
 *
 *   1. INSERT of a TABLE_LIKE entity not already in entity_link -> a new
 *      Iceberg table appeared in Polaris that pg_lake doesn't know about.
 *      Create a foreign table that points at it and run the existing
 *      sync_iceberg_metadata_from_external_write() function.
 *
 *   2. UPDATE of a TABLE_LIKE entity already in entity_link, where the
 *      metadata-location changed -> external commit. Update
 *      lake_iceberg.tables_internal.metadata_location and re-run sync.
 *
 *   3. DELETE of a TABLE_LIKE entity in entity_link -> external client
 *      dropped the table. Drop the corresponding foreign table.
 *
 * Loop prevention: bail out immediately when set_suppressed(true) is
 * already in effect (the outbound trigger is in flight).
 */


/*
 * inbound_sync_update applies a metadata_location change from Polaris to
 * the pg_lake catalog. The PG_LAKE_TABLE function
 * sync_iceberg_metadata_from_external_write() does the actual schema +
 * data file resync; we just push the new location into tables_internal
 * so it has somewhere to read from.
 */
CREATE FUNCTION lake_polaris.inbound_sync_update(
    p_table_oid regclass,
    p_metadata_location text)
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
BEGIN
    UPDATE lake_iceberg.tables_internal
       SET previous_metadata_location = metadata_location,
           metadata_location = p_metadata_location
     WHERE table_name = p_table_oid;

    PERFORM lake_table.sync_iceberg_metadata_from_external_write(p_table_oid);

    UPDATE lake_polaris.entity_link
       SET last_seen_metadata_location = p_metadata_location
     WHERE pg_table_oid = p_table_oid;
END;
$function$;


/*
 * inbound_sync_delete drops a foreign table that has been removed from
 * Polaris.
 */
CREATE FUNCTION lake_polaris.inbound_sync_delete(p_table_oid regclass)
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
DECLARE
    v_namespace_name text;
    v_table_name text;
    v_relkind char;
BEGIN
    SELECT n.nspname, c.relname, c.relkind
      INTO v_namespace_name, v_table_name, v_relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = p_table_oid;

    IF NOT FOUND THEN
        DELETE FROM lake_polaris.entity_link WHERE pg_table_oid = p_table_oid;
        RETURN;
    END IF;

    IF v_relkind = 'f' THEN
        EXECUTE format('DROP FOREIGN TABLE %I.%I',
                       v_namespace_name, v_table_name);
    ELSE
        EXECUTE format('DROP TABLE %I.%I',
                       v_namespace_name, v_table_name);
    END IF;

    DELETE FROM lake_polaris.entity_link WHERE pg_table_oid = p_table_oid;
END;
$function$;


/*
 * inbound_sync_insert creates a new pg_lake foreign table for an Iceberg
 * table that just appeared in Polaris. Reads the schema from the Iceberg
 * metadata file and translates the columns to Postgres types.
 *
 * v0.1 limitation: only top-level scalar columns are supported. Tables
 * whose Iceberg schema includes nested types (struct/list/map) will fail
 * here; the user has to register them through pg_lake's normal
 * CREATE TABLE flow first.
 */
CREATE FUNCTION lake_polaris.inbound_sync_insert(
    p_namespace_name text,
    p_table_name text,
    p_metadata_location text,
    p_polaris_entity_id bigint,
    p_polaris_namespace_id bigint)
    RETURNS regclass
    LANGUAGE plpgsql
    AS $function$
DECLARE
    v_metadata jsonb;
    v_current_schema_id int;
    v_schema jsonb;
    v_field jsonb;
    v_columns text := '';
    v_column_def text;
    v_pg_type text;
    v_iceberg_type text;
    v_new_oid regclass;
BEGIN
    /* read the Iceberg metadata file */
    v_metadata := lake_iceberg.metadata(p_metadata_location);

    v_current_schema_id := (v_metadata->>'current-schema-id')::int;

    /* find the current schema */
    FOR v_schema IN SELECT jsonb_array_elements(v_metadata->'schemas')
    LOOP
        IF (v_schema->>'schema-id')::int = v_current_schema_id THEN
            EXIT;
        END IF;
    END LOOP;

    IF v_schema IS NULL THEN
        RAISE EXCEPTION 'pg_lake_polaris: could not find current schema in %',
            p_metadata_location;
    END IF;

    FOR v_field IN SELECT jsonb_array_elements(v_schema->'fields')
    LOOP
        IF jsonb_typeof(v_field->'type') <> 'string' THEN
            RAISE EXCEPTION 'pg_lake_polaris: nested types not supported in v0.1 (column %)',
                v_field->>'name';
        END IF;

        v_iceberg_type := v_field->>'type';
        v_pg_type := CASE v_iceberg_type
            WHEN 'boolean' THEN 'boolean'
            WHEN 'int' THEN 'integer'
            WHEN 'long' THEN 'bigint'
            WHEN 'float' THEN 'real'
            WHEN 'double' THEN 'double precision'
            WHEN 'date' THEN 'date'
            WHEN 'time' THEN 'time'
            WHEN 'timestamp' THEN 'timestamp'
            WHEN 'timestamptz' THEN 'timestamptz'
            WHEN 'string' THEN 'text'
            WHEN 'uuid' THEN 'uuid'
            WHEN 'binary' THEN 'bytea'
            ELSE NULL
        END;

        IF v_pg_type IS NULL THEN
            RAISE EXCEPTION 'pg_lake_polaris: unsupported Iceberg type % for column %',
                v_iceberg_type, v_field->>'name';
        END IF;

        v_column_def := format('%I %s', v_field->>'name', v_pg_type);

        IF v_columns = '' THEN
            v_columns := v_column_def;
        ELSE
            v_columns := v_columns || ', ' || v_column_def;
        END IF;
    END LOOP;

    /*
     * Create the foreign table. It's read-only in v0.1: external clients
     * own the data, pg_lake serves queries. The catalog_name option points
     * at the database so pg_lake_iceberg recognizes this as a managed
     * catalog table.
     */
    EXECUTE format(
        'CREATE FOREIGN TABLE %I.%I (%s) SERVER pg_lake_iceberg OPTIONS (read_only ''true'')',
        p_namespace_name, p_table_name, v_columns);

    v_new_oid := format('%I.%I', p_namespace_name, p_table_name)::regclass;

    /* register in pg_lake's iceberg catalog so it knows about the table */
    INSERT INTO lake_iceberg.tables_internal
        (table_name, metadata_location, has_custom_location)
    VALUES
        (v_new_oid, p_metadata_location, true);

    /* sync schema and data files */
    PERFORM lake_table.sync_iceberg_metadata_from_external_write(v_new_oid);

    /* link */
    INSERT INTO lake_polaris.entity_link
        (pg_table_oid, polaris_entity_id, polaris_namespace_id,
         last_seen_metadata_location)
    VALUES
        (v_new_oid, p_polaris_entity_id, p_polaris_namespace_id,
         p_metadata_location);

    RETURN v_new_oid;
END;
$function$;


/*
 * inbound_trigger fires AFTER row changes on polaris_schema.entities.
 * Filtered by the WHEN clause to only fire on TABLE_LIKE entities for the
 * configured catalog.
 */
CREATE FUNCTION lake_polaris.inbound_trigger()
    RETURNS trigger
    LANGUAGE plpgsql
    AS $function$
DECLARE
    v_old_suppress boolean;
    v_mapping lake_polaris.catalog_mapping%ROWTYPE;
    v_link lake_polaris.entity_link%ROWTYPE;
    v_metadata_location text;
    v_new_internal jsonb;
    v_old_internal jsonb;
BEGIN
    IF lake_polaris.is_suppressed() THEN
        RETURN NULL;
    END IF;

    SELECT * INTO v_mapping
    FROM lake_polaris.catalog_mapping
    WHERE pg_database = current_database();

    IF NOT FOUND OR NOT v_mapping.enabled THEN
        RETURN NULL;
    END IF;

    /* only react to events for our catalog */
    IF (TG_OP IN ('INSERT', 'UPDATE') AND NEW.catalog_id <> v_mapping.polaris_catalog_id)
       OR (TG_OP = 'DELETE' AND OLD.catalog_id <> v_mapping.polaris_catalog_id) THEN
        RETURN NULL;
    END IF;

    v_old_suppress := lake_polaris.set_suppressed(true);

    BEGIN
        IF TG_OP = 'DELETE' THEN
            SELECT * INTO v_link FROM lake_polaris.entity_link
             WHERE polaris_entity_id = OLD.id;
            IF FOUND THEN
                PERFORM lake_polaris.inbound_sync_delete(v_link.pg_table_oid);
            END IF;

        ELSIF TG_OP = 'INSERT' THEN
            /* a new TABLE_LIKE entity. If we already linked it, this was
             * our own outbound write; suppression should have prevented
             * the trigger anyway, so just no-op. */
            PERFORM 1 FROM lake_polaris.entity_link
             WHERE polaris_entity_id = NEW.id;
            IF NOT FOUND THEN
                v_new_internal := NEW.internal_properties::jsonb;
                v_metadata_location := v_new_internal->>'metadata-location';
                IF v_metadata_location IS NOT NULL THEN
                    PERFORM lake_polaris.inbound_sync_insert(
                        v_new_internal->>'parent-namespace',
                        NEW.name,
                        v_metadata_location,
                        NEW.id,
                        NEW.parent_id);
                END IF;
            END IF;

        ELSIF TG_OP = 'UPDATE' THEN
            SELECT * INTO v_link FROM lake_polaris.entity_link
             WHERE polaris_entity_id = NEW.id;

            IF FOUND THEN
                v_new_internal := NEW.internal_properties::jsonb;
                v_metadata_location := v_new_internal->>'metadata-location';

                /* Skip if the metadata-location matches what we last wrote
                 * out — that's our own round-trip. */
                IF v_metadata_location IS NOT NULL
                   AND v_metadata_location IS DISTINCT FROM v_link.last_seen_metadata_location
                THEN
                    PERFORM lake_polaris.inbound_sync_update(
                        v_link.pg_table_oid, v_metadata_location);
                END IF;
            END IF;
        END IF;
    EXCEPTION WHEN OTHERS THEN
        PERFORM lake_polaris.set_suppressed(v_old_suppress);
        RAISE;
    END;

    PERFORM lake_polaris.set_suppressed(v_old_suppress);
    RETURN NULL;
END;
$function$;


/*
 * The inbound trigger lives on polaris_schema.entities, which doesn't
 * necessarily exist when CREATE EXTENSION runs. Create / drop it from
 * register_catalog / unregister_catalog instead.
 */


/*
 * register_catalog pins the local database to a named Polaris catalog.
 * It snapshots the current pg_lake_polaris.realm_id GUC into the mapping
 * row so a session-level SET cannot silently change the binding.
 *
 * After registering, all existing pg_lake Iceberg tables are mirrored into
 * Polaris via bootstrap_existing_tables().
 */
CREATE FUNCTION lake_polaris.register_catalog(p_polaris_catalog_name text)
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
DECLARE
    v_realm_id text;
    v_catalog_id bigint;
BEGIN
    /*
     * Touch a C function so the .so is loaded and _PG_init has registered
     * the realm_id GUC before we read it. Without this, the first call to
     * register_catalog in a fresh session fails on current_setting().
     */
    PERFORM lake_polaris.is_suppressed();
    v_realm_id := current_setting('pg_lake_polaris.realm_id');

    SELECT id INTO v_catalog_id
    FROM polaris_schema.entities
    WHERE realm_id = v_realm_id
      AND type_code = lake_polaris.type_code_catalog()
      AND name = p_polaris_catalog_name;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'pg_lake_polaris: catalog "%" not found in realm "%"',
            p_polaris_catalog_name, v_realm_id;
    END IF;

    INSERT INTO lake_polaris.catalog_mapping
        (pg_database, polaris_catalog_name, polaris_catalog_id,
         polaris_realm_id, enabled)
    VALUES
        (current_database(), p_polaris_catalog_name, v_catalog_id,
         v_realm_id, true)
    ON CONFLICT (pg_database) DO UPDATE
        SET polaris_catalog_name = EXCLUDED.polaris_catalog_name,
            polaris_catalog_id = EXCLUDED.polaris_catalog_id,
            polaris_realm_id = EXCLUDED.polaris_realm_id,
            enabled = true;

    /*
     * Install the inbound trigger now that polaris_schema.entities is
     * guaranteed to exist. Postgres does not allow TG_OP inside a trigger
     * WHEN clause, so we install separate triggers per operation, each
     * filtered by type_code on the appropriate row variant. The function
     * dispatches on TG_OP internally.
     */
    EXECUTE $trg$
        CREATE OR REPLACE TRIGGER inbound_trigger_iud
        AFTER INSERT OR UPDATE ON polaris_schema.entities
        FOR EACH ROW WHEN (NEW.type_code = 7)
        EXECUTE FUNCTION lake_polaris.inbound_trigger()
    $trg$;
    EXECUTE $trg$
        CREATE OR REPLACE TRIGGER inbound_trigger_d
        AFTER DELETE ON polaris_schema.entities
        FOR EACH ROW WHEN (OLD.type_code = 7)
        EXECUTE FUNCTION lake_polaris.inbound_trigger()
    $trg$;

    PERFORM lake_polaris.bootstrap_existing_tables();
END;
$function$;


CREATE FUNCTION lake_polaris.unregister_catalog()
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
BEGIN
    UPDATE lake_polaris.catalog_mapping
       SET enabled = false
     WHERE pg_database = current_database();

    /* Drop the inbound triggers. The outbound trigger stays — it short-
     * circuits via the disabled mapping row. */
    EXECUTE 'DROP TRIGGER IF EXISTS inbound_trigger_iud ON polaris_schema.entities';
    EXECUTE 'DROP TRIGGER IF EXISTS inbound_trigger_d ON polaris_schema.entities';
END;
$function$;


/*
 * bootstrap_existing_tables iterates lake_iceberg.tables_internal and
 * calls outbound_sync for each row, so registration of a Polaris catalog
 * picks up tables that were created before the extension was enabled.
 *
 * Idempotent — outbound_sync already does upsert-style behavior keyed off
 * entity_link.
 */
CREATE FUNCTION lake_polaris.bootstrap_existing_tables()
    RETURNS void
    LANGUAGE plpgsql
    AS $function$
DECLARE
    v_old_suppress boolean;
    v_oid regclass;
BEGIN
    v_old_suppress := lake_polaris.set_suppressed(true);

    BEGIN
        FOR v_oid IN
            SELECT table_name FROM lake_iceberg.tables_internal
        LOOP
            PERFORM lake_polaris.outbound_sync(v_oid);
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        PERFORM lake_polaris.set_suppressed(v_old_suppress);
        RAISE;
    END;

    PERFORM lake_polaris.set_suppressed(v_old_suppress);
END;
$function$;
