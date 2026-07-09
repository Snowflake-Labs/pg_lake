-- Upgrade script for pg_lake_benchmark from 3.3 to 3.4

-- Restrict benchmark data-generation functions to the lake_write role.
-- Any authenticated user could previously pass
-- an unbounded scale_factor / iteration_count to the shared pgduck_server
-- process, exhausting RAM/CPU/disk for all tenants.  Generation already
-- required lake_write at the C level via CheckURLWriteAccess(); this brings
-- the SQL privilege in line with that.
REVOKE ALL ON FUNCTION lake_tpch.gen(
    location TEXT, table_type lake_tpch.table_type,
    scale_factor FLOAT4, iteration_count INT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_tpch.gen(
    location TEXT, table_type lake_tpch.table_type,
    scale_factor FLOAT4, iteration_count INT
) TO lake_write;

REVOKE ALL ON FUNCTION lake_tpch.gen_partitioned(
    location TEXT, table_type lake_tpch.table_type,
    scale_factor FLOAT4, iteration_count INT,
    lineitem_partition_by TEXT, customer_partition_by TEXT,
    nation_partition_by TEXT, orders_partition_by TEXT,
    part_partition_by TEXT, partsupp_partition_by TEXT,
    region_partition_by TEXT, supplier_partition_by TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_tpch.gen_partitioned(
    location TEXT, table_type lake_tpch.table_type,
    scale_factor FLOAT4, iteration_count INT,
    lineitem_partition_by TEXT, customer_partition_by TEXT,
    nation_partition_by TEXT, orders_partition_by TEXT,
    part_partition_by TEXT, partsupp_partition_by TEXT,
    region_partition_by TEXT, supplier_partition_by TEXT
) TO lake_write;

REVOKE ALL ON FUNCTION lake_tpcds.gen(
    location TEXT, table_type lake_tpcds.table_type,
    scale_factor FLOAT4
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION lake_tpcds.gen(
    location TEXT, table_type lake_tpcds.table_type,
    scale_factor FLOAT4
) TO lake_write;
