import tempfile
import pytest
import psycopg2
from utils_pytest import *
import pyarrow as pa
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import (
    StringType,
    DoubleType,
    LongType,
    TimestampType,
    MapType,
    StructType,
    ListType,
    IntegerType,
)
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import *

from pyiceberg.catalog import load_catalog
from pyiceberg.transforms import IdentityTransform
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.table.refs import MAIN_BRANCH, SnapshotRef, SnapshotRefType

import pyarrow as pa
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import StringType, DoubleType, MapType

from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import (
    StringType,
    DoubleType,
    FloatType,
    StructType,
    NestedField as IcebergNestedField,
)


def complex_table(iceberg_catalog):
    schema = Schema(
        NestedField(1, "text_col", StringType(), required=False),
        NestedField(2, "double_col_1", DoubleType(), required=False),
        NestedField(
            field_id=3,
            name="column_sizes",
            field_type=StructType(
                IcebergNestedField(4, "size_key", StringType(), required=False),
                IcebergNestedField(5, "size_value", DoubleType(), required=False),
            ),
            required=False,
        ),
        NestedField(
            field_id=6,
            name="location",
            field_type=ListType(
                element_id=7,
                element=StructType(
                    NestedField(
                        field_id=8,
                        name="latitude",
                        field_type=DoubleType(),
                        required=False,
                    ),
                    NestedField(
                        field_id=9,
                        name="longitude",
                        field_type=DoubleType(),
                        required=False,
                    ),
                ),
                element_required=False,
            ),
            required=False,
        ),
        NestedField(
            field_id=10,
            name='location"_lookup',
            field_type=MapType(
                key_id=11,
                key_type=StringType(),
                value_id=12,
                value_type=StructType(
                    NestedField(
                        field_id=13,
                        name="x\"''.x",
                        field_type=FloatType(),
                        required=False,
                    ),
                    NestedField(
                        field_id=14, name="y", field_type=FloatType(), required=False
                    ),
                ),
                value_required=False,
            ),
            required=False,
        ),
        NestedField(
            field_id=2222,
            name="locations",
            field_type=ListType(
                element_id=220,
                element=StructType(
                    NestedField(
                        field_id=200, name="x", field_type=FloatType(), required=False
                    ),
                    NestedField(
                        field_id=201, name="y", field_type=FloatType(), required=False
                    ),
                ),
                element_required=False,
            ),
            required=False,
        ),
        NestedField(
            field_id=333,
            name="person",
            field_type=StructType(
                NestedField(
                    field_id=30, name="name", field_type=StringType(), required=False
                ),
                NestedField(
                    field_id=31, name="age", field_type=IntegerType(), required=False
                ),
            ),
            required=False,
        ),
    )

    partition_spec = PartitionSpec(
        PartitionField(
            source_id=5, field_id=1000, transform=IdentityTransform(), name="col_uuid"
        ),
        spec_id=0,
    )
    sort_order = SortOrder(SortField(source_id=4, transform=IdentityTransform()))
    table_properties = {
        "write.format.default": "parquet",
        "compression.codec": "gzip",
        "read.split.target-size": "134217728",  # 128 MB
        "write.target-file-size-bytes": "536870912",  # 512 MB
        "escaped \" ''' tetext' ": "asdesc \" asdasd -~''",
    }

    iceberg_table = iceberg_catalog.create_table(
        identifier="public.complex_table",
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/complex_table",
        sort_order=sort_order,
        properties=table_properties,
    )

    pa_schema = pa.schema(
        [
            pa.field("text_col", pa.string()),
            pa.field("double_col_1", pa.float64()),
            pa.field(
                "column_sizes",
                pa.struct(
                    [
                        pa.field("size_key", pa.string()),
                        pa.field("size_value", pa.float64()),
                    ]
                ),
            ),
            pa.field(
                "location",
                pa.list_(
                    pa.struct(
                        [
                            pa.field("latitude", pa.float64()),
                            pa.field("longitude", pa.float64()),
                        ]
                    )
                ),
            ),
            pa.field(
                'location"_lookup',
                pa.map_(
                    pa.string(),
                    pa.struct(
                        [pa.field("x\"''.x", pa.float32()), pa.field("y", pa.float32())]
                    ),
                ),
            ),
            pa.field(
                "locations",
                pa.list_(
                    pa.struct(
                        [pa.field("x", pa.float32()), pa.field("y", pa.float32())]
                    )
                ),
            ),
            pa.field(
                "person",
                pa.struct([pa.field("name", pa.string()), pa.field("age", pa.int32())]),
            ),
        ]
    )

    # Create data with the defined schema

    data_gen = lambda text_col_val: pa.Table.from_pydict(
        {
            "text_col": [text_col_val, "some text"],
            "double_col_1": [52.371807, 52.371807],
            "column_sizes": [
                {"size_key": "key1", "size_value": 1.0},
                {"size_key": "key2", "size_value": 2.0},
            ],
            "location": [
                [{"latitude": 52.370216, "longitude": 4.895168}],
                [{"latitude": 52.370216, "longitude": 4.895168}],
            ],
            'location"_lookup': [
                {
                    "key''|'\"1": {"x\"''.x": 12.34, "y": 56.78},
                    "key2": {"x\"''.x": 98.76, "y": 54.32},
                },
                {
                    "key1": {"x\"''.x": 12.34, "y": 56.78},
                    "key2": {"x\"''.x": 98.76, "y": 54.32},
                },
            ],
            "locations": [
                [{"x": 12.34, "y": 56.78}, {"x": 98.76, "y": 54.32}],
                [{"x": 12.34, "y": 56.78}, {"x": 98.76, "y": 54.32}],
            ],
            "person": [
                {"name": "John Doe", "age": 30},
                {"name": "Jane Doe", "age": 25},
            ],
        },
        schema=pa_schema,
    )

    # iceberg_table.manage_snapshots().create_tag(snapshot_id, "tag123").commit()

    # Append data to Iceberg table
    iceberg_table.append(data_gen("txt1"))
    iceberg_table.append(data_gen("txt2"))
    iceberg_table.append(data_gen("txt3"))
    iceberg_table.append(data_gen("txt4"))

    # Delete a row
    iceberg_table.delete(delete_filter="text_col == 'txt2'")

    # Delete other rows
    iceberg_table.delete()

    create_tag(iceberg_catalog)
    create_branch(iceberg_catalog)

    return iceberg_table


def create_tag(catalog: Catalog) -> None:
    identifier = "public.complex_table"
    tbl = catalog.load_table(identifier)
    # assert len(tbl.history()) > 3
    print(tbl.history)
    tag_snapshot_id = tbl.history()[0].snapshot_id
    tbl.manage_snapshots().create_tag(
        snapshot_id=tag_snapshot_id, tag_name="tag123"
    ).commit()
    assert tbl.metadata.refs["tag123"] == SnapshotRef(
        snapshot_id=tag_snapshot_id, snapshot_ref_type="tag"
    )


def create_branch(catalog: Catalog) -> None:
    identifier = "public.complex_table"
    tbl = catalog.load_table(identifier)
    assert len(tbl.history()) > 2
    branch_snapshot_id = tbl.history()[-2].snapshot_id
    tbl.manage_snapshots().create_branch(
        snapshot_id=branch_snapshot_id, branch_name="branch123"
    ).commit()
    assert tbl.metadata.refs["branch123"] == SnapshotRef(
        snapshot_id=branch_snapshot_id, snapshot_ref_type="branch"
    )


def pg_lake_iceberg_datafile_paths(superuser_conn, metadata_location, is_delete=False):
    is_delete_str = "true" if is_delete else "false"
    res = run_query(
        f"""
        SELECT lake_iceberg.datafile_paths_from_table_metadata('{metadata_location}', {is_delete_str});
""",
        superuser_conn,
    )

    datafile_paths = res[0][0]
    return datafile_paths


def test_iceberg_catalog(
    create_reserialize_helper_functions, iceberg_catalog, superuser_conn, s3
):

    iceberg_table = complex_table(iceberg_catalog)
    metadata_location = str(iceberg_table.metadata_location)
    read_s3_operations(s3, iceberg_table.metadata_location)

    manifest_list_location = manifest_list_file_location(
        superuser_conn, metadata_location
    )
    read_s3_operations(s3, manifest_list_location, is_text=False)

    manifest_locations = manifest_file_locations(superuser_conn, manifest_list_location)
    for manifest_location in manifest_locations:
        read_s3_operations(s3, manifest_location, is_text=False)

    # this table is created by pyicberg, we have not hooked into anything
    # we read the table and save for comparison
    table_data_with_pyiceberg_metadata = iceberg_catalog.load_table(
        "public.complex_table"
    )
    pyiceberg_result = read_complex_table(
        iceberg_catalog, table_data_with_pyiceberg_metadata
    )

    # now, we use our own logic to regenerate the metadata.json,
    # and push it to s3 instead of the original metadata.json generated via pyiceberg
    regenerate_metadata_json(superuser_conn, metadata_location, s3)
    read_s3_operations(s3, iceberg_table.metadata_location)

    # regenerate manifest list file and push it to s3 by replacing the original one
    regenerate_manifest_list_file(superuser_conn, manifest_list_location, s3)
    read_s3_operations(s3, manifest_list_location, is_text=False)

    # regenerate manifest files and push them to s3 by replacing the original ones
    for manifest_location in manifest_locations:
        regenerate_manifest_file(superuser_conn, manifest_location, s3)
        read_s3_operations(s3, manifest_location, is_text=False)

    # reload the metadata, and query the table
    table_data_with_pg_lake_iceberg_metadata = iceberg_catalog.load_table(
        "public.complex_table"
    )
    pg_lake_result = read_complex_table(
        iceberg_catalog, table_data_with_pg_lake_iceberg_metadata
    )

    iceberg_catalog.drop_table("public.complex_table")

    assert pg_lake_result == pyiceberg_result


def test_iceberg_datafiles(
    create_reserialize_helper_functions,
    spark_generated_iceberg_test,
    superuser_conn,
    s3,
    duckdb_conn,
):
    install_duckdb_extension(duckdb_conn, "iceberg")

    iceberg_table_metadata_location = (
        "s3://"
        + TEST_BUCKET
        + "/spark_test/public/spark_generated_iceberg_test/metadata/00009-5c29aedb-463b-4b80-b0d5-c1d7fc957770.metadata.json"
    )

    duckdb_conn.execute(
        f"""
                       select file_path from iceberg_metadata('{iceberg_table_metadata_location}')
                        where manifest_content = 'DATA'
                        order by file_path;
                       """,
    )
    result = duckdb_conn.fetchall()
    duckdb_expected_data_files = [row[0] for row in result]

    duckdb_conn.execute(
        f"""
                       select file_path from iceberg_metadata('{iceberg_table_metadata_location}')
                        where manifest_content = 'DELETE'
                        order by file_path;
                       """,
    )
    result = duckdb_conn.fetchall()
    duckdb_expected_delete_files = [row[0] for row in result]

    pg_lake_expected_data_files = pg_lake_iceberg_datafile_paths(
        superuser_conn, iceberg_table_metadata_location, is_delete=False
    )
    pg_lake_expected_delete_files = pg_lake_iceberg_datafile_paths(
        superuser_conn, iceberg_table_metadata_location, is_delete=True
    )

    assert len(pg_lake_expected_data_files) > 0
    assert len(pg_lake_expected_delete_files) > 0
    assert sorted(pg_lake_expected_data_files) == sorted(duckdb_expected_data_files)
    assert sorted(pg_lake_expected_delete_files) == sorted(duckdb_expected_delete_files)


def read_complex_table(iceberg_catalog, iceberg_table):
    # Create a reader for the table
    reader = iceberg_table.scan().to_arrow()
    return reader
