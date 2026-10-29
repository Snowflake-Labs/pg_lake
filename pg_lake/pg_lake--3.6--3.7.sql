-- Upgrade script for pg_lake from 3.6 to 3.7

-- extension_base has no USAGE grant, so a tenant role cannot read the worker
-- id sequence directly. That lockdown is what keeps run_attached() out of
-- reach. This runs as the extension owner and returns only the sequence state
-- the next registration will collide with.
CREATE FUNCTION lake.worker_id_sequence(
	OUT last_value bigint,
	OUT is_called boolean,
	OUT next_value bigint,
	OUT max_worker_id bigint,
	OUT worker_count bigint,
	OUT next_value_taken boolean,
	OUT workers text)
RETURNS record
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
	SELECT s.last_value,
		   s.is_called,
		   CASE WHEN s.is_called THEN s.last_value + 1 ELSE s.last_value END,
		   coalesce(w.max_worker_id, 0),
		   w.worker_count,
		   EXISTS (
			   SELECT 1
			   FROM extension_base.workers x
			   WHERE x.worker_id = CASE WHEN s.is_called
										 THEN s.last_value + 1
										 ELSE s.last_value END),
		   coalesce(w.workers, '')
	FROM extension_base.workers_worker_id_seq s
	CROSS JOIN LATERAL (
		SELECT max(worker_id) AS max_worker_id,
			   count(*) AS worker_count,
			   string_agg(worker_id::text || ':' || worker_name, ', '
						  ORDER BY worker_id) AS workers
		FROM extension_base.workers
	) w;
$$;

COMMENT ON FUNCTION lake.worker_id_sequence()
IS 'extension_base.workers against the sequence that assigns worker_id';

REVOKE ALL ON FUNCTION lake.worker_id_sequence() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake.worker_id_sequence() TO pg_monitor;
