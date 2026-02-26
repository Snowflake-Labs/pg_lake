import pytest
from utils_pytest import TEST_BUCKET, s3_upload_dir


def delta_sample_delta_table_folder_path():
    from pathlib import Path

    return str(Path(__file__).parent.parent / "sample" / "data" / "delta")


@pytest.fixture(scope="module")
def sample_delta_table(s3):
    src_dir = delta_sample_delta_table_folder_path() + "/people_countries_delta_dask"
    dest_path = "test_delta_tables/people_countries"
    dest_url = f"s3://{TEST_BUCKET}/{dest_path}"

    s3_upload_dir(s3, src_dir, TEST_BUCKET, dest_path)
    yield dest_url

    # TODO: cleanup?
