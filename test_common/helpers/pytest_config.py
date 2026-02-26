"""Pytest hooks, command-line options, and session-level configuration."""

import os

import pytest

from . import server_params


def pg_lake_version_from_env():
    return os.getenv("PG_LAKE_GIT_VERSION", "0.0.0")


def pytest_addoption(parser):
    if "__initialized" not in parser.extra_info:
        parser.addoption(
            "--installcheck",
            action="store_true",
            default=False,
            help="installcheck: True or False",
        )
        parser.addoption(
            "--isolationtester",
            action="store_true",
            default=False,
            help="isolationtester: True or False",
        )
        parser.addoption(
            "--pglog",
            action="store",
            default=None,
            help="set log_min_messages on generated clusters.",
        )
        parser.extra_info["__initialized"] = True


@pytest.fixture(scope="session")
def installcheck(request):
    myopt = request.config.getoption("--installcheck")

    if myopt:
        os.environ["INSTALLCHECK"] = "1"

        # Point server_params at the real (already-running) servers so
        # that every fixture reading these values picks up the correct
        # connection details.
        server_params.PGDUCK_PORT = 5332
        server_params.DUCKDB_DATABASE_FILE_PATH = "/tmp/duckdb.db"
        server_params.PGDUCK_UNIX_DOMAIN_PATH = "/tmp"
        server_params.PGDUCK_CACHE_DIR = "/tmp/cache"

        server_params.PG_DATABASE = os.getenv("PGDATABASE", "regression")
        server_params.PG_USER = os.getenv("PGUSER", "postgres")
        server_params.PG_PASSWORD = os.getenv("PGPASSWORD", "postgres")
        server_params.PG_PORT = os.getenv("PGPORT", "5432")
        server_params.PG_HOST = os.getenv("PGHOST", "localhost")
        server_params.PG_DIR = "/tmp/pg_installcheck_tests"

    return myopt


@pytest.fixture(scope="session")
def isolationtester(request):
    return request.config.getoption("--isolationtester")


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and before performing collection.
    We use this hook to set the environment variable if '--setenv' is provided.
    """
    config = session.config
    log_level = config.getoption("--pglog")
    if log_level is not None:
        if log_level not in [
            "panic",
            "log",
            "notice",
            "fatal",
            "error",
            "warning",
            "debug",
            "debug1",
            "debug2",
            "debug3",
            "debug4",
            "debug5",
        ]:
            raise Exception("invalid --pglog setting: expected valid log_min_messages")

        os.environ["LOG_MIN_MESSAGES"] = log_level
