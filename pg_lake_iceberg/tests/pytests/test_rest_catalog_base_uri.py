import pytest

from utils_pytest import *


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn):
    run_command(
        """
        CREATE OR REPLACE FUNCTION lake_iceberg.resolve_rest_catalog_base_uri(endpoint TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$resolve_rest_catalog_base_uri$function$;
""",
        superuser_conn,
    )

    yield

    run_command(
        "DROP FUNCTION lake_iceberg.resolve_rest_catalog_base_uri;",
        superuser_conn,
    )


# (input endpoint, expected normalized base URI)
BASE_URI_CASES = [
    # Bare host -> legacy Polaris mount path appended (backward compat).
    ("https://polaris.example.com", "https://polaris.example.com/api/catalog"),
    (
        "https://polaris.example.com:8181",
        "https://polaris.example.com:8181/api/catalog",
    ),
    # Historical scheme-less "host:port" form (as set via the GUC) also
    # counts as bare and gets the legacy prefix.
    ("polaris:8181", "polaris:8181/api/catalog"),
    # Explicit mount paths are used verbatim -> unblocks other catalogs.
    ("https://host/api/catalog", "https://host/api/catalog"),  # Polaris, explicit
    ("https://host/catalog", "https://host/catalog"),  # Lakekeeper
    ("https://host/iceberg", "https://host/iceberg"),  # Nessie
    (
        "https://host/api/2.1/unity-catalog/iceberg",
        "https://host/api/2.1/unity-catalog/iceberg",
    ),  # Unity Catalog
    # A single trailing slash is stripped before the path check, so a bare
    # host with a trailing slash still gets the legacy prefix...
    ("https://host/", "https://host/api/catalog"),
    # ...and an explicit path keeps only its trailing slash stripped.
    ("https://host/catalog/", "https://host/catalog"),
    # Empty string is returned unchanged (ValidateRestCatalogOptions later
    # raises "rest_endpoint not configured").
    ("", ""),
]


@pytest.mark.parametrize("endpoint,expected", BASE_URI_CASES)
def test_resolve_rest_catalog_base_uri(
    create_helper_functions, superuser_conn, endpoint, expected
):
    cur = superuser_conn.cursor()
    try:
        cur.execute(
            "SELECT lake_iceberg.resolve_rest_catalog_base_uri(%s)", (endpoint,)
        )
        result = cur.fetchone()
    finally:
        cur.close()

    assert result[0] == expected
