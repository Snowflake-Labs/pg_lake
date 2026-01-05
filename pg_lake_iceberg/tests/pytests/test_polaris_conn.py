from utils_pytest import *
import server_params


# pg_conn is to start Polaris server
def test_polaris_catalog_running(pg_conn, polaris_session, installcheck):

    if installcheck:
        return

    """Fail fast if Polaris is not healthy."""
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/config?warehouse={server_params.PG_DATABASE}"
    resp = polaris_session.get(url, timeout=1)
    assert resp.ok, f"Polaris is not running: {resp.status_code} {resp.text}"


def test_polaris_catalog_test_http_get(
    pg_conn, polaris_session, installcheck, create_http_helper_functions
):
    if installcheck:
        return
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/config?warehouse={server_params.PG_DATABASE}"
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

    # yay, success!
    assert res[0][0] == 200


# tests http_post, http_put, http_get, http_head and http_delete
def test_polaris_catalog_test_namespace_and_policy(
    pg_conn, polaris_session, installcheck, create_http_helper_functions
):
    if installcheck:
        return

    # first, lets create a namespace
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces"
    token = get_polaris_access_token()

    namespace_name = "prod/team_a"
    url_encoded_namespace_name = run_query(
        f"SELECT lake_iceberg.url_encode('{namespace_name}')", pg_conn
    )[0][0]

    json_payload = f'{{"namespace": ["{namespace_name}"]}}'
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

    # yay, success!
    assert res[0][0] == 200

    # make sure we can get back recently created namespace
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces"

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_with_retry(
         'GET',
         '{url}',
         headers => ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )

    # yay, success!
    assert res[0][0] == 200
    assert [namespace_name] in json.loads(res[0][1])["namespaces"]

    # now, lets create a policy under that namespace
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/polaris/v1/{server_params.PG_DATABASE}/namespaces/{url_encoded_namespace_name}/policies"
    policy_name = "snapshot-expiry"
    url_encoded_policy_name = run_query(
        f"SELECT lake_iceberg.url_encode('{policy_name}')", pg_conn
    )[0][0]

    json_payload = f'{{"name": "{policy_name}", "type": "system.snapshot-expiry", "description": "Expire old snapshots", "content": "{{\\"version\\":\\"2025-02-03\\",\\"enable\\":true,\\"config\\":{{\\"min_snapshot_to_keep\\":1,\\"max_snapshot_age_days\\":7,\\"max_ref_age_days\\":14}} }}"}}'
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

    # yay, success!
    assert res[0][0] == 200

    # now, lets change policy content (enable: false)
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/polaris/v1/{server_params.PG_DATABASE}/namespaces/{url_encoded_namespace_name}/policies/{url_encoded_policy_name}"

    json_payload = '{"current-policy-version": 0, "content": "{\\"version\\":\\"2025-02-03\\",\\"enable\\":false,\\"config\\":{\\"min_snapshot_to_keep\\":1,\\"max_snapshot_age_days\\":7,\\"max_ref_age_days\\":14} }"}'
    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_put(
         '{url}',
         '{json_payload}',
         ARRAY['Authorization: Bearer {token}',
               'Content-Type: application/json']);
    """,
        pg_conn,
    )

    # yay, success!
    assert res[0][0] == 200

    # make sure we can get back recently created policy
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/polaris/v1/{server_params.PG_DATABASE}/namespaces/{url_encoded_namespace_name}/policies/{url_encoded_policy_name}"
    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_get(
         '{url}',
         ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )

    # yay, success!
    assert res[0][0] == 200
    assert json.loads(res[0][1])["policy"]["name"] == policy_name
    assert json.loads(res[0][1])["policy"]["description"] == None  # PUT made it empty
    assert '"enable":false' in json.loads(res[0][1])["policy"]["content"]

    # make sure we can use head to ensure the namespace exists
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces/{url_encoded_namespace_name}"
    res = run_query(
        f"""
                SELECT *
                FROM   lake_iceberg.test_http_head(
                         '{url}',
                ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )
    # success!
    assert res[0][0] == 204

    # now, lets also DELETE the policy
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/polaris/v1/{server_params.PG_DATABASE}/namespaces/{url_encoded_namespace_name}/policies/{url_encoded_policy_name}"

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_delete(
         '{url}',
         ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )

    # yay, success, the API defines 204 as success!
    assert res[0][0] == 204

    # now, let's also DELETE the same namespace
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces/{url_encoded_namespace_name}"

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_delete(
         '{url}',
         ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )
    # yay, success, the API defines 204 as success!
    assert res[0][0] == 204

    # make sure we deleted the namespace
    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/{server_params.PG_DATABASE}/namespaces"

    res = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_get(
         '{url}',
         ARRAY['Authorization: Bearer {token}']);
        """,
        pg_conn,
    )

    # yay, success!
    assert res[0][0] == 200
    assert [url_encoded_namespace_name] not in json.loads(res[0][1])["namespaces"]


def test_http_errors(
    pg_conn,
    polaris_session,
    installcheck,
    create_http_helper_functions,
):
    if installcheck:
        return

    url = f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}/api/catalog/v1/config?warehouse={server_params.PG_DATABASE}"

    # no token
    error = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_get(
         '{url}',
         ARRAY[]::text[]);
        """,
        pg_conn,
        raise_error=False,
    )

    assert "401" in error

    pg_conn.rollback()

    # dns failure
    url = f"http://wronghost:{server_params.POLARIS_PORT}/api/catalog/v1/config?warehouse={server_params.PG_DATABASE}"

    error = run_query(
        f"""
        SELECT *
        FROM lake_iceberg.test_http_get(
         '{url}',
         ARRAY[]::text[]);
        """,
        pg_conn,
        raise_error=False,
    )

    assert "Could not resolve host" in error or "Resolving timed out after" in error

    pg_conn.rollback()
