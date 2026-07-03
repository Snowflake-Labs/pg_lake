import os


# Skeleton placeholder test so `make check` collects at least one test. Real
# suites (merge-on-read reconciliation under concurrent flush, frontier
# maintenance, CustomScan aggregate pushdown) land with the implementation;
# see DESIGN.md, "Testing strategy".
EXT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def test_skeleton_files_present():
    for name in (
        "pg_lake_timeseries.control",
        "pg_lake_timeseries--3.4.sql",
        "DESIGN.md",
    ):
        assert os.path.exists(os.path.join(EXT_DIR, name)), name
