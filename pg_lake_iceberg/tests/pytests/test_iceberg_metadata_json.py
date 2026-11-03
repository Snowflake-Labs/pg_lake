import pytest
import psycopg2
from utils_pytest import *
from deepdiff import DeepDiff
import json


def collect_metadata_files():

    folder_path = iceberg_metadata_json_folder_path()
    file_names = []
    for filename in os.listdir(folder_path):

        if filename.endswith(".json"):
            file_names.append((filename, os.path.join(folder_path, filename)))

    # Sort file_names by the filename
    file_names = sorted(file_names, key=lambda x: x[0])

    return file_names


def collect_manifest_files():

    folder_path = iceberg_metadata_manifest_folder_path()
    file_names = []
    for filename in os.listdir(folder_path):

        if filename.endswith(".avro"):
            file_names.append((filename, os.path.join(folder_path, filename)))

    # Sort file_names by the filename
    file_names = sorted(file_names, key=lambda x: x[0])

    return file_names


FILE_DATA = collect_metadata_files()
ids = [file_names[0] for file_names in FILE_DATA]


@pytest.mark.parametrize("filename,filepath", FILE_DATA, ids=ids)
def test_metadata_json(create_helper_functions, filename, filepath, superuser_conn, s3):

    s3_key = f"test_metadata_json/{filename}"
    s3.upload_file(filepath, TEST_BUCKET, s3_key)
    s3_url = f"s3://{TEST_BUCKET}/{s3_key}"

    command = (
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('{s3_url}')::json"
    )

    result = run_query(command, superuser_conn)
    returned_json = result[0][0]
    assert_valid_json(returned_json)
    original_json = read_json(filepath)
    assert_jsons_equivalent(original_json, returned_json)


MANIFESTS_FILE_DATA = collect_manifest_files()
ids = [file_names[0] for file_names in MANIFESTS_FILE_DATA]


@pytest.mark.parametrize("filename,filepath", MANIFESTS_FILE_DATA, ids=ids)
def test_copy_local_manifests_file_to_s3(
    create_helper_functions, filename, filepath, superuser_conn, s3
):

    location = f"s3://{TEST_BUCKET}/local_push/{filename}"
    command = f"SELECT lake_iceberg.pg_lake_copy_file('{filepath}', '{location}')"
    run_command(command, superuser_conn)

    returned_bytes = read_s3_operations(s3, location, is_text=False)
    original_json = read_binary(filepath)
    assert returned_bytes == original_json


ids = [file_names[0] for file_names in FILE_DATA]


@pytest.mark.parametrize("filename,filepath", FILE_DATA, ids=ids)
def test_copy_local_metadata_file_to_s3(
    create_helper_functions, filename, filepath, superuser_conn, s3
):

    location = f"s3://{TEST_BUCKET}/local_push/{filename}"
    command = f"SELECT lake_iceberg.pg_lake_copy_file('{filepath}', '{location}')"
    run_command(command, superuser_conn)

    returned_json = normalize_json(read_s3_operations(s3, location))
    assert_valid_json(returned_json)
    original_json = read_json(filepath)
    assert_jsons_equivalent(original_json, returned_json)


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn):

    run_command(
        f"""
        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_iceberg_table_metadata(metadataUri TEXT)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$reserialize_iceberg_table_metadata$function$;

        CREATE OR REPLACE FUNCTION lake_iceberg.pg_lake_copy_file(localFile TEXT, s3Uri text)
        RETURNS void
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$pg_lake_copy_file$function$;

""",
        superuser_conn,
    )

    yield

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION lake_iceberg.reserialize_iceberg_table_metadata;
        DROP FUNCTION lake_iceberg.pg_lake_copy_file;
""",
        superuser_conn,
    )


def corrupt_missing_field_type(metadata):
    del metadata["schemas"][0]["fields"][0]["type"]


def corrupt_schemas_scalar_elements(metadata):
    metadata["schemas"] = [1, 2]


def corrupt_schemas_array_elements(metadata):
    metadata["schemas"] = [[1, 2]]


def corrupt_schemas_object(metadata):
    metadata["schemas"] = {"a": 1}


def corrupt_field_type_array(metadata):
    metadata["schemas"][0]["fields"][0]["type"] = ["struct", 1]


def corrupt_refs_array(metadata):
    metadata["refs"] = [1, 2]


def corrupt_ref_value_array(metadata):
    metadata["refs"] = {"main": [1, 2]}


@pytest.mark.parametrize(
    "corrupt",
    [
        corrupt_missing_field_type,
        corrupt_schemas_scalar_elements,
        corrupt_schemas_array_elements,
        corrupt_schemas_object,
        corrupt_field_type_array,
        corrupt_refs_array,
        corrupt_ref_value_array,
    ],
)
def test_corrupt_metadata_json_errors(
    create_helper_functions, corrupt, superuser_conn, s3, tmp_path
):
    # metadata with a value of the wrong JSON type is rejected, not read as
    # the expected type
    _, filepath = FILE_DATA[0]
    metadata = read_json(filepath)
    corrupt(metadata)

    local = tmp_path / "metadata.json"
    local.write_text(json.dumps(metadata))
    s3_key = f"test_corrupt_metadata_json/{corrupt.__name__}.metadata.json"
    s3.upload_file(str(local), TEST_BUCKET, s3_key)

    # the helper function is not committed, so keep it across the error
    run_command("SAVEPOINT corrupt_metadata", superuser_conn)
    error = run_query(
        f"SELECT lake_iceberg.reserialize_iceberg_table_metadata('s3://{TEST_BUCKET}/{s3_key}')",
        superuser_conn,
        raise_error=False,
    )
    assert "Iceberg" in error
    run_command("ROLLBACK TO SAVEPOINT corrupt_metadata", superuser_conn)

    # the backend survived
    assert run_query("SELECT 1", superuser_conn) == [[1]]
