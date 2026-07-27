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
# ResolveRestCatalogBaseUri strips one trailing slash and returns the
# endpoint verbatim.  The full mount path must be included in rest_endpoint.
BASE_URI_CASES = [
    # Bare host: returned unchanged (no path appended since 3.5).
    ("https://polaris.example.com", "https://polaris.example.com"),
    ("https://polaris.example.com:8181", "https://polaris.example.com:8181"),
    # Historical scheme-less "host:port" form: also returned unchanged.
    ("polaris:8181", "polaris:8181"),
    # Explicit mount paths are used verbatim (trailing slash stripped).
    ("https://host/api/catalog", "https://host/api/catalog"),  # Polaris
    ("https://host/catalog", "https://host/catalog"),  # Lakekeeper
    ("https://host/iceberg", "https://host/iceberg"),  # Nessie
    (
        "https://host/api/2.1/unity-catalog/iceberg",
        "https://host/api/2.1/unity-catalog/iceberg",
    ),  # Unity Catalog
    # A trailing slash is stripped regardless of whether a path follows.
    ("https://host/", "https://host"),
    ("https://host/catalog/", "https://host/catalog"),
    # Double trailing slash: one slash stripped, result keeps a trailing
    # slash.  The URL template then emits "https://host//v1/..." which
    # produces a server-side 404 rather than a silent wrong result.
    ("https://host//", "https://host/"),
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
