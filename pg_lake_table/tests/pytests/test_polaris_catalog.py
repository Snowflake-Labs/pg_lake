from utils_pytest import *
import server_params
from urllib.parse import quote
from urllib.parse import urlencode
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    IntegerType,
    LongType,
    DoubleType,
    StringType,
    BinaryType,
    FixedType,
    NestedField,
    ListType,
    StructType,
)
import pyarrow
from datetime import datetime, date, timezone
from urllib.parse import quote, quote_plus
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionSpec, PartitionField
import json


# pg_conn is to start Polaris server
def test_polaris_catalog_running(pg_conn, s3, polaris_session, installcheck):

    if installcheck:
        return

    """Fail fast if Polaris is not healthy."""
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/config?warehouse={server_params.PG_DATABASE}"
    resp = polaris_session.get(url, timeout=1)
    assert resp.ok, f"Polaris is not running: {resp.status_code} {resp.text}"


namespaces = [
    "regular_name",
    "regular..!!**(());;//??::@@&&==++$$,,#name",
    "Special-Table!_With.Multiple_Uses_Of@Chars#-Here~And*Here!name",
    "!~*();/?:@&=+$,#",
]


@pytest.mark.parametrize("namespace", namespaces)
def test_create_namespace(
    pg_conn,
    set_polaris_gucs,
    with_default_location,
    s3,
    polaris_session,
    installcheck,
    create_http_helper_functions,
    namespace,
):

    if installcheck:
        return

    run_command(f'''CREATE SCHEMA "{namespace}"''', pg_conn)
    pg_conn.commit()

    run_command(
        f"""CREATE TABLE "{namespace}".tbl(a int) USING iceberg WITH (catalog='rest');""",
        pg_conn,
    )
    pg_conn.commit()

    # no-op, just to make sure nothing is broken
    run_command_outside_tx([f"""VACUUM "{namespace}".tbl"""], pg_conn)

    encoded_namespace = run_query(
        f"SELECT lake_iceberg.url_encode_path('{namespace}')", pg_conn
    )[0][0]
    location = get_namespace_location(encoded_namespace, pg_conn, installcheck)

    assert (
        location.lower()
        == f"s3://testbucketcdw/{server_params.PG_DATABASE}/{namespace}/".lower()
    )

    res = run_command(
        f"""CREATE TABLE "{namespace}".tbl_err(a int) USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='none')""",
        pg_conn,
        raise_error=False,
    )
    assert "does not exist in the rest catalog while creating on catalog" in str(res)
    pg_conn.rollback()

    res = run_command(
        f"""CREATE TABLE "{namespace}".tbl_err(a int) USING iceberg WITH (catalog='rest', read_only=True, catalog_name='none')""",
        pg_conn,
        raise_error=False,
    )
    assert "does not exist in the rest catalog while creating on catalog" in str(res)
    pg_conn.rollback()

    res = run_command(
        f"""CREATE TABLE "{namespace}".tbl_err(a int) USING iceberg WITH (catalog='rest', read_only=False, catalog_name='none', catalog_namespace='none', catalog_table_name='none')""",
        pg_conn,
        raise_error=False,
    )
    assert "writable rest catalog iceberg tables do not" in str(res)
    pg_conn.rollback()

    res = run_command(
        f"""CREATE TABLE "{namespace}".tbl_err(a int) USING iceberg WITH (catalog='rest', catalog_name='none')""",
        pg_conn,
        raise_error=False,
    )
    assert "writable rest catalog iceberg tables do not" in str(res)
    pg_conn.rollback()

    run_command(f"""DROP SCHEMA "{namespace}" CASCADE""", pg_conn)
    pg_conn.commit()


@pytest.mark.parametrize("namespace", namespaces)
def test_create_namespace_in_tx(
    pg_conn,
    set_polaris_gucs,
    with_default_location,
    s3,
    polaris_session,
    installcheck,
    create_http_helper_functions,
    namespace,
):

    if installcheck:
        return

    run_command(f'''CREATE SCHEMA "{namespace}"''', pg_conn)
    run_command(f'''CREATE SCHEMA "{namespace}_2"''', pg_conn)

    run_command(
        f"""CREATE TABLE "{namespace}".tbl_10(a int) USING iceberg WITH (catalog='rest');""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""CREATE TABLE "{namespace}_2".tbl_11(a int) USING iceberg WITH (catalog='rest');""",
        pg_conn,
    )
    pg_conn.commit()

    encoded_namespace = run_query(
        f"SELECT lake_iceberg.url_encode_path('{namespace}')", pg_conn
    )[0][0]
    location = get_namespace_location(encoded_namespace, pg_conn, installcheck)

    assert (
        location.lower()
        == f"s3://testbucketcdw/{server_params.PG_DATABASE}/{namespace}/".lower()
    )

    run_command(f"""DROP SCHEMA "{namespace}" CASCADE""", pg_conn)
    pg_conn.commit()


# even if we rollback, the namespace stays
@pytest.mark.parametrize("namespace", namespaces)
def test_create_namespace_rollback(
    pg_conn,
    set_polaris_gucs,
    with_default_location,
    s3,
    polaris_session,
    installcheck,
    create_http_helper_functions,
    namespace,
):

    if installcheck:
        return

    run_command(f'''CREATE SCHEMA "{namespace}"''', pg_conn)

    run_command(
        f"""CREATE TABLE "{namespace}".tbl_20(a int) USING iceberg WITH (catalog='rest');""",
        pg_conn,
    )

    pg_conn.rollback()

    encoded_namespace = run_query(
        f"SELECT lake_iceberg.url_encode_path('{namespace}')", pg_conn
    )[0][0]
    location = get_namespace_location(encoded_namespace, pg_conn, installcheck)

    assert (
        location.lower()
        == f"s3://testbucketcdw/{server_params.PG_DATABASE}/{namespace}/".lower()
    )


existing_namespaces = [
    "regular_nsp_name",
    "nonregular_nsp!~*()name:$Uses_Of@",
]
existing_table_names = [
    "regular_tbl_name",
    "nonregular_tbl!~*()name:$Uses_Of@",
]


@pytest.mark.parametrize("tbl_name", existing_table_names)
@pytest.mark.parametrize("namespace", existing_namespaces)
@pytest.mark.parametrize("partition_by", [False, True])
@pytest.mark.parametrize("drop_columns", [False, True])
@pytest.mark.parametrize("external_catalog_names", [False, True])
def test_register_existing_table(
    pg_conn,
    set_polaris_gucs,
    with_default_location,
    s3,
    polaris_session,
    installcheck,
    create_http_helper_functions,
    grant_access_to_tables_internal,
    partition_by,
    drop_columns,
    external_catalog_names,
    tbl_name,
    namespace,
):

    if installcheck:
        return

    catalog_namespace_name = f"{namespace}"
    catalog_table_name = f"{tbl_name}"

    # sometimes setting not setting search_path might
    # cause issues, so let's run with setting
    # we refrain adding one more level of combination
    set_search_path = partition_by or drop_columns or external_catalog_names

    # table names in pg is different than
    # rest catalog
    if external_catalog_names:
        namespace = f"{namespace}_pg"
        tbl_name = f"{tbl_name}_pg"

    rest_catalog = create_iceberg_rest_catalog(catalog_namespace_name)

    iceberg_table = create_rest_catalog_table(
        catalog_table_name,
        f"{catalog_namespace_name}",
        rest_catalog,
        partition_by,
        drop_columns,
    )

    run_command(f'''CREATE SCHEMA "{namespace}"''', pg_conn)
    if set_search_path:
        run_command(f'''SET search_path TO "{namespace}"''', pg_conn)

    # table names in pg is different than
    # rest catalog
    if external_catalog_names:

        if set_search_path:
            run_command(
                f"""CREATE TABLE "{tbl_name}"() USING iceberg WITH (catalog='rest', read_only=True, catalog_name='{server_params.PG_DATABASE}', catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
                pg_conn,
            )
        else:
            run_command(
                f"""CREATE TABLE "{namespace}"."{tbl_name}"() USING iceberg WITH (catalog='rest', read_only=True, catalog_name='{server_params.PG_DATABASE}', catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
                pg_conn,
            )
    else:

        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"() USING iceberg WITH (catalog='rest', read_only=True)""",
            pg_conn,
        )

    run_command(
        f"""CREATE TYPE "{namespace}".type_details AS (created_by text)""", pg_conn
    )

    pg_conn.commit()

    res = run_query(
        f"""SELECT * FROM "{namespace}"."{tbl_name}" ORDER BY \"lat lat\" DESC""",
        pg_conn,
    )

    # {tbl_name} table have four rows
    assert len(res) == 5
    assert res[0] == ["city_4", 4, 4, "(user_4)"]

    # does not support load_from + external_catalog
    run_command(
        f"COPY (SELECT i FROM generate_series(0,100)i) TO 's3://{TEST_BUCKET}/external_data.parquet' ",
        pg_conn,
    )
    run_command(f'''DROP TABLE "{namespace}"."{tbl_name}"''', pg_conn)
    pg_conn.commit()

    err = run_command(
        f"""CREATE TABLE "{namespace}"."{tbl_name}"() USING iceberg WITH (catalog='rest', read_only=True, load_from='s3://{TEST_BUCKET}/external_data.parquet', catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
        pg_conn,
        raise_error=False,
    )
    assert "does not allow inserts" in str(err)
    pg_conn.rollback()

    # CTAS not supported
    err = run_command(
        f"""CREATE TABLE "{namespace}"."{tbl_name}" USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}') AS SELECT 1""",
        pg_conn,
        raise_error=False,
    )
    assert "does not allow inserts" in str(err)
    pg_conn.rollback()

    # should be able to re-create
    if external_catalog_names:
        run_command(
            f"""CREATE TABLE IF NOT EXISTS "{namespace}"."{tbl_name}"() USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
            pg_conn,
        )

        # if not exists should simply skip
        run_command(
            f"""CREATE TABLE IF NOT EXISTS "{namespace}"."{tbl_name}"() USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
            pg_conn,
        )

    else:

        run_command(
            f"""CREATE TABLE IF NOT EXISTS "{namespace}"."{tbl_name}"() USING iceberg WITH (catalog='rest', read_only=True)""",
            pg_conn,
        )

        # if not exists should simply skip
        run_command(
            f"""CREATE TABLE IF NOT EXISTS "{namespace}"."{tbl_name}"() USING iceberg WITH (catalog='rest', read_only=True)""",
            pg_conn,
        )

    # don't register to tables_internal
    res = run_query(
        f"""SELECT count(*) FROM lake_iceberg.tables_internal WHERE table_name = '"{namespace}"."{tbl_name}"'::regclass """,
        pg_conn,
    )
    assert res == [[0]]

    pg_conn.commit()

    res = run_query(
        f"""SELECT * FROM "{namespace}"."{tbl_name}" ORDER BY \"lat lat\" DESC""",
        pg_conn,
    )

    # {tbl_name} table have four rows
    assert len(res) == 5
    assert res[0] == ["city_4", 4, 4, "(user_4)"]

    # ingest more data via the rest catalog
    data = pyarrow.Table.from_pylist(
        [
            {
                "city": "city_5",
                f"""lat lat""": float(5),
                "long": float(5),
                "details": {"created_by": f"user_5"},
            }
        ],
    )
    iceberg_table.append(data)

    # make sure we can follow the changes
    if set_search_path:
        res = run_query(
            f"""SELECT * FROM "{tbl_name}" WHERE city = 'city_5' ORDER BY \"lat lat\" ASC""",
            pg_conn,
        )
    else:
        res = run_query(
            f"""SELECT * FROM "{namespace}"."{tbl_name}" WHERE city = 'city_5' ORDER BY \"lat lat\" ASC""",
            pg_conn,
        )

    # make sure we can properly prune data
    assert len(res) == 1
    assert res[0] == ["city_5", 5, 5, "(user_5)"]

    # let's update city_3 and city_5
    updated = pyarrow.Table.from_pylist(
        [{"city": "city_3", f"""lat lat""": 99.0, "long": 88.0}]
    )
    iceberg_table.overwrite(updated, overwrite_filter=EqualTo("city", "city_3"))
    updated = pyarrow.Table.from_pylist(
        [{"city": "city_5", f"""lat lat""": 99.0, "long": 88.0}]
    )
    iceberg_table.overwrite(updated, overwrite_filter=EqualTo("city", "city_5"))

    # make sure we can follow the changes
    res = run_query(
        f"""SELECT city FROM "{namespace}"."{tbl_name}" WHERE \"lat lat\" = 99.0 ORDER BY city ASC""",
        pg_conn,
    )

    # make sure we can properly prune data
    assert len(res) == 2
    assert res[0] == ["city_3"]
    assert res[1] == ["city_5"]

    # we cannot modify the table
    cmds = [
        (
            f"""INSERT INTO "{namespace}"."{tbl_name}" (city) VALUES ('Istanbul')""",
            "does not allow inserts",
        ),
        (f'''DELETE FROM "{namespace}"."{tbl_name}"''', "does not allow deletes"),
        (
            f"""UPDATE "{namespace}"."{tbl_name}" SET city = 'Istanbul' """,
            "does not allow updates",
        ),
        (
            f'''TRUNCATE "{namespace}"."{tbl_name}"''',
            "modifications on read-only iceberg tables are not supported",
        ),
        (
            f'''INSERT INTO "{namespace}"."{tbl_name}" SELECT * FROM "{namespace}"."{tbl_name}"''',
            "does not allow inserts",
        ),
        (
            f"""ALTER TABLE "{namespace}"."{tbl_name}" ADD COLUMN x INT""",
            "modifications on read-only iceberg tables are not supported",
        ),
    ]
    for cmd, cmd_error in cmds:
        err = run_command(cmd, pg_conn, raise_error=False)
        assert cmd_error in str(err)
        pg_conn.rollback()

    pg_conn.autocommit = True

    pg_conn.notices.clear()
    run_command(f'''VACUUM "{namespace}"."{tbl_name}"''', pg_conn)
    assert any("WARNING:" in notice for notice in pg_conn.notices)
    pg_conn.rollback()

    pg_conn.autocommit = False

    # adding a column to external table breaks further reads in Postgres
    iceberg_table.update_schema().add_column(
        "population", IntegerType(), doc="Population of the city"
    ).commit()

    err = run_query(
        f"""SELECT * FROM "{namespace}"."{tbl_name}" """,
        pg_conn,
        raise_error=False,
    )
    assert "Schema mismatch between Iceberg and Postgres" in str(err)
    pg_conn.rollback()

    # drop column to match with Postgres again
    iceberg_table.update_schema().delete_column("population").commit()
    run_query(f"""SELECT * FROM "{namespace}"."{tbl_name}" """, pg_conn)

    # now, drop the table & re-create with different column names
    run_command(f'''DROP TABLE "{namespace}"."{tbl_name}"''', pg_conn)

    pg_conn.commit()

    if external_catalog_names:
        res = run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(a int, b pg_class) USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
            pg_conn,
            raise_error=False,
        )
        assert "table types are not supported as columns" in str(res)
        pg_conn.rollback()

    else:
        res = run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(a int, b pg_class) USING iceberg WITH (catalog='rest', read_only=True)""",
            pg_conn,
            raise_error=False,
        )
        assert "table types are not supported as columns" in str(res)
        pg_conn.rollback()

    if external_catalog_names:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, lat float, looong float, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
            pg_conn,
        )
    else:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, lat float, looong float, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True)""",
            pg_conn,
        )

    err = run_query(
        f"""SELECT * FROM "{namespace}"."{tbl_name}" ORDER BY lat DESC""",
        pg_conn,
        raise_error=False,
    )
    assert "Schema mismatch between Iceberg and Postgres" in str(err)
    pg_conn.rollback()

    if external_catalog_names:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, \"lat lat\" float, long float NOT NULL, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
            pg_conn,
        )
    else:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, \"lat lat\" float, long float NOT NULL, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True)""",
            pg_conn,
        )

    err = run_query(
        f"""SELECT * FROM "{namespace}"."{tbl_name}" ORDER BY \"lat lat\" DESC""",
        pg_conn,
        raise_error=False,
    )
    assert "Schema mismatch between Iceberg and Postgres" in str(err)
    pg_conn.rollback()

    if external_catalog_names:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, \"lat lat\" float, long float DEFAULT 15.6, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
            pg_conn,
        )
    else:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, \"lat lat\" float, long float DEFAULT 15.6, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True)""",
            pg_conn,
        )

    err = run_query(
        f"""SELECT * FROM "{namespace}"."{tbl_name}" ORDER BY \"lat lat\" DESC""",
        pg_conn,
        raise_error=False,
    )
    assert "Schema mismatch between Iceberg and Postgres" in str(err)
    pg_conn.rollback()

    # now, drop the table & re-create with different type
    if external_catalog_names:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, \"lat lat\" float, long int, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
            pg_conn,
        )
    else:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, \"lat lat\" float, long int, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True)""",
            pg_conn,
        )

    err = run_query(
        f"""SELECT * FROM "{namespace}"."{tbl_name}" ORDER BY \"lat lat\" DESC""",
        pg_conn,
        raise_error=False,
    )
    assert "Schema mismatch between Iceberg and Postgres" in str(err)
    pg_conn.rollback()

    # finally, we can create a table with the exact
    # same schema as external catalog, and that should work
    if external_catalog_names:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, \"lat lat\" float, long float, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True, catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
            pg_conn,
        )
    else:
        run_command(
            f"""CREATE TABLE "{namespace}"."{tbl_name}"(city text, \"lat lat\" float, long float, details "{namespace}".type_details) USING iceberg WITH (catalog='rest', read_only=True)""",
            pg_conn,
        )

    err = run_query(
        f"""SELECT * FROM "{namespace}"."{tbl_name}" ORDER BY \"lat lat\" DESC""",
        pg_conn,
    )
    run_command(f"""DROP SCHEMA "{namespace}" CASCADE""", pg_conn)
    pg_conn.commit()
    nuke_rest_catalog(rest_catalog, catalog_namespace_name)

    if set_search_path:
        run_command(f"""RESET search_path""", pg_conn)


def test_register_existing_table_renames(
    pg_conn,
    set_polaris_gucs,
    with_default_location,
    s3,
    polaris_session,
    installcheck,
    create_http_helper_functions,
    grant_access_to_tables_internal,
):

    if installcheck:
        return

    catalog_namespace_name = "tmp_namespace_rename"
    rest_catalog = create_iceberg_rest_catalog(catalog_namespace_name)
    run_command(f"""CREATE SCHEMA "{catalog_namespace_name}_pg" """, pg_conn)

    catalog_table_name = f"tmp_table_rename"
    iceberg_table = create_rest_catalog_table(
        catalog_table_name,
        f"{catalog_namespace_name}",
        rest_catalog,
        False,
        False,
    )

    run_command(
        f"""CREATE TABLE "{catalog_namespace_name}_pg"."{catalog_namespace_name}_pg"() USING iceberg WITH (catalog='rest', read_only=True, catalog_name='{server_params.PG_DATABASE}', catalog_namespace='{catalog_namespace_name}', catalog_table_name='{catalog_table_name}')""",
        pg_conn,
    )
    pg_conn.commit()
    # basic verification
    res = run_query(
        f"""SELECT * FROM "{catalog_namespace_name}_pg"."{catalog_namespace_name}_pg" """,
        pg_conn,
    )
    assert len(res) == 5

    rename_rest_catalog_table(
        pg_conn,
        catalog_namespace_name,
        f"{catalog_table_name}",
        f"{catalog_table_name}_new",
    )

    # now, the table's catalog_table_name diverged, so should fail
    res = run_query(
        f"""SELECT * FROM "{catalog_namespace_name}_pg"."{catalog_namespace_name}_pg" """,
        pg_conn,
        raise_error=False,
    )
    assert "HTTP request failed (HTTP 404)" in str(res)
    assert "Table does not exist" in str(res)
    pg_conn.rollback()

    # now, set catalog_table_name
    run_command(
        f"""ALTER FOREIGN TABLE "{catalog_namespace_name}_pg"."{catalog_namespace_name}_pg" OPTIONS (SET catalog_table_name '{catalog_table_name}_new') """,
        pg_conn,
    )

    # and, we should be able to read the table back
    res = run_query(
        f"""SELECT * FROM "{catalog_namespace_name}_pg"."{catalog_namespace_name}_pg" """,
        pg_conn,
    )
    assert len(res) == 5

    # now, set catalog_namespace is not allowed to be changed
    err = run_command(
        f"""ALTER FOREIGN TABLE "{catalog_namespace_name}_pg"."{catalog_namespace_name}_pg" OPTIONS (SET catalog_name '{catalog_table_name}_new') """,
        pg_conn,
        raise_error=False,
    )
    assert "The following table options can be changed" in str(err)
    pg_conn.rollback()

    # now, set catalog_namespace is not allowed to be changed
    err = run_command(
        f"""ALTER FOREIGN TABLE "{catalog_namespace_name}_pg"."{catalog_namespace_name}_pg" OPTIONS (SET catalog_namespace '{catalog_table_name}_new') """,
        pg_conn,
        raise_error=False,
    )
    assert "The following table options can be changed" in str(err)
    pg_conn.rollback()

    # now, dropping catalog_table_name is not allowed to be changed
    err = run_command(
        f"""ALTER FOREIGN TABLE "{catalog_namespace_name}_pg"."{catalog_namespace_name}_pg" OPTIONS (DROP catalog_table_name) """,
        pg_conn,
        raise_error=False,
    )
    assert '"catalog_table_name" option is required for' in str(err)
    pg_conn.rollback()

    run_command(f"""DROP SCHEMA "{catalog_namespace_name}_pg" CASCADE""", pg_conn)
    pg_conn.commit()
    nuke_rest_catalog(rest_catalog, catalog_namespace_name)


def rename_rest_catalog_table(pg_conn, catalog_namespace_name, old_name, new_name):
    if isinstance(catalog_namespace_name, str):
        ns_list = [p for p in catalog_namespace_name.split(".") if p]
    else:
        ns_list = list(catalog_namespace_name)

    url = (
        f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}"
        f"/api/catalog/v1/{server_params.PG_DATABASE}/tables/rename"
    )
    token = get_polaris_access_token()

    payload = {
        "source": {"namespace": ns_list, "name": old_name},
        "destination": {"namespace": ns_list, "name": new_name},
    }

    json_payload = json.dumps(payload)

    # Escape for SQL literals
    url_sql = url.replace("'", "''")
    body_sql = json_payload.replace("'", "''")
    auth_sql = f"Authorization: Bearer {token}".replace("'", "''")

    sql = f"""
        SELECT *
        FROM lake_iceberg.test_http_post(
            '{url_sql}',
            '{body_sql}',
            ARRAY['{auth_sql}', 'Content-Type: application/json']
        );
    """

    res = run_query(sql, pg_conn)
    status = res[0][0]
    if status not in (200, 204):
        body = res[0][1] if len(res[0]) > 1 else None
        raise RuntimeError(f"Rename failed: HTTP {status}, body={body}")

    return status


def nuke_rest_catalog(catalog, namespace):
    tables = catalog.list_tables(namespace)
    for table in tables:
        catalog.drop_table(table)
    catalog.drop_namespace(namespace)


def create_rest_catalog_table(
    tbl_name, namespace, iceberg_catalog, partition_by=False, drop_columns=False
):

    if drop_columns:
        schema = Schema(
            NestedField(1, "drop_col_1", DoubleType(), required=False),
            NestedField(2, "city", StringType(), required=False),
            NestedField(3, f"""lat lat""", DoubleType(), required=False),
            NestedField(4, "long", DoubleType(), required=False),
            NestedField(5, "drop_col_2", DoubleType(), required=False),
            NestedField(
                field_id=6,
                name="details",
                field_type=StructType(
                    NestedField(
                        field_id=7,
                        name="created_by",
                        field_type=StringType(),
                        required=False,
                    ),
                ),
                required=False,
            ),
        )
    else:
        schema = Schema(
            NestedField(1, "city", StringType(), required=False),
            NestedField(2, f"""lat lat""", DoubleType(), required=False),
            NestedField(3, "long", DoubleType(), required=False),
            NestedField(
                field_id=4,
                name="details",
                field_type=StructType(
                    NestedField(
                        field_id=5,
                        name="created_by",
                        field_type=StringType(),
                        required=False,
                    ),
                ),
                required=False,
            ),
        )

    part_spec = PartitionSpec(fields=[])
    if partition_by:
        part_spec = PartitionSpec(
            PartitionField(
                source_id=2 if drop_columns else 1,
                field_id=1000,
                transform="identity",
                name="city_part",
            )
        )

    iceberg_table = iceberg_catalog.create_table(
        identifier=f"{namespace}.{tbl_name}",
        schema=schema,
        partition_spec=part_spec,
    )

    if drop_columns:
        iceberg_table.update_schema().delete_column("drop_col_1").commit()
        iceberg_table.update_schema().delete_column("drop_col_2").commit()

    rows = []
    for i in range(0, 5):
        rows.append(
            {
                "city": f"city_{i}",
                f"""lat lat""": float(i),
                "long": float(i),
                "details": {"created_by": f"user_{i}"},
            }
        )

    data = pyarrow.Table.from_pylist(rows)

    iceberg_table.append(data)

    return iceberg_table


def get_namespace_location(encoded_namespace, pg_conn, installcheck):
    if installcheck:
        return
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces/{encoded_namespace}"
    token = get_polaris_access_token()

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_get(
         '{url}',
         ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )
    assert res[0][0] == 200
    return json.loads(res[0][1])["properties"]["location"]


def set_namespace_location(encoded_namespace, pg_conn):
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces/{encoded_namespace}/properties"
    token = get_polaris_access_token()

    json_payload = f"""
    {{
      "updates": {{
        "location": "s3://testbucketcdw/another_place"
      }}
    }}
    """

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_post(
         '{url}',
         '{json_payload}',
         ARRAY['Authorization: Bearer {token}',
               'Content-Type: application/json']);
    """,
        pg_conn,
    )

    assert res[0][0] == 200


@pytest.fixture(scope="function")
def grant_access_to_tables_internal(
    extension,
    app_user,
    superuser_conn,
):
    run_command(
        f"""GRANT SELECT ON lake_iceberg.tables_internal TO {app_user};""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""REVOKE SELECT ON lake_iceberg.tables_internal FROM {app_user};""",
        superuser_conn,
    )
    superuser_conn.commit()


def test_rest_catalog_uppercase_columns(
    pg_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
):
    """Test querying a REST catalog table with all uppercase column names using CREATE TABLE () syntax."""

    if installcheck:
        return

    # Create a namespace for the test
    namespace = "test_uppercase_columns"
    rest_catalog = create_iceberg_rest_catalog(namespace)

    # Define schema with all UPPERCASE column names
    schema = Schema(
        NestedField(1, "ID", LongType(), required=False),
        NestedField(2, "NAME", StringType(), required=False),
        NestedField(3, "VALUE", DoubleType(), required=False),
    )

    # Create the table in REST catalog
    table_name = "uppercase_table"
    iceberg_table = rest_catalog.create_table(
        identifier=f"{namespace}.{table_name}",
        schema=schema,
    )

    # Insert some data using PyArrow
    data = pyarrow.Table.from_pylist(
        [
            {"ID": 1, "NAME": "Alice", "VALUE": 100.5},
            {"ID": 2, "NAME": "Bob", "VALUE": 200.75},
            {"ID": 3, "NAME": "Charlie", "VALUE": 300.25},
        ]
    )
    iceberg_table.append(data)

    # Create schema in PostgreSQL
    run_command(f"""CREATE SCHEMA "{namespace}" """, pg_conn)
    pg_conn.commit()

    # Create table in PostgreSQL using CREATE TABLE () syntax to infer columns
    run_command(
        f"""
        CREATE TABLE "{namespace}".test_uppercase ()
        USING iceberg
        WITH (
            catalog='rest',
            read_only=True,
            catalog_table_name='{table_name}'
        )
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Verify the columns were correctly inferred
    columns = run_query(
        f"""
        SELECT attname
        FROM pg_attribute
        WHERE attrelid = '"{namespace}".test_uppercase'::regclass
        AND attnum > 0
        ORDER BY attnum
        """,
        pg_conn,
    )
    assert len(columns) == 3
    # PostgreSQL should have lowercased the column names (unless they were quoted)
    # since Iceberg column names are case-sensitive, pg_lake should preserve them as-is
    assert columns[0][0] == "ID"
    assert columns[1][0] == "NAME"
    assert columns[2][0] == "VALUE"

    # Query the data
    result = run_query(
        f"""
        SELECT "ID", "NAME", "VALUE"
        FROM "{namespace}".test_uppercase
        ORDER BY "ID"
        """,
        pg_conn,
    )

    assert len(result) == 3
    assert result[0] == [1, "Alice", 100.5]
    assert result[1] == [2, "Bob", 200.75]
    assert result[2] == [3, "Charlie", 300.25]

    # Clean up
    rest_catalog.drop_table(f"{namespace}.{table_name}")
    rest_catalog.drop_namespace(namespace)
