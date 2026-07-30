import os

import pytest
from utils_pytest import *

# Names of the extensions we install outside of the share directory.
# They deliberately do not start with pg_extension_base_test, because they
# are not installed in the share directory like the other test extensions.
CONTROL_PATH_EXT = "control_path_test_ext"
CONTROL_PATH_DEP = "control_path_test_dep"
CONTROL_PATH_PRELOAD = "control_path_test_preload"


def write_extension(directory, name, control_lines, script=None):
    with open(f"{directory}/{name}.control", "w") as control_file:
        control_file.write("\n".join(control_lines) + "\n")

    if script is not None:
        with open(f"{directory}/{name}--1.0.sql", "w") as script_file:
            script_file.write(script + "\n")


@pytest.fixture(scope="module")
def extension_control_directory(tmp_path_factory):
    """
    Installs SQL-only extensions in a directory that is not the share
    directory, in the layout that PostgreSQL expects from an
    extension_control_path entry: <entry>/extension/<name>.control.

    Returns the path to add to extension_control_path.
    """
    base_directory = tmp_path_factory.mktemp("extension_control_path")
    extension_directory = base_directory / "extension"
    extension_directory.mkdir()

    # PostgreSQL reads the files as the user that runs the server, which is
    # the user that runs the tests, but the directories are traversed by
    # every backend, so keep them readable.
    os.chmod(base_directory, 0o755)
    os.chmod(extension_directory, 0o755)

    write_extension(
        extension_directory,
        CONTROL_PATH_DEP,
        [
            "comment = 'dependency installed via extension_control_path'",
            "default_version = '1.0'",
            "relocatable = true",
        ],
        f"CREATE FUNCTION {CONTROL_PATH_DEP}() RETURNS int LANGUAGE sql AS 'SELECT 1';",
    )

    write_extension(
        extension_directory,
        CONTROL_PATH_EXT,
        [
            "comment = 'extension installed via extension_control_path'",
            "default_version = '1.0'",
            "relocatable = true",
            f"requires = '{CONTROL_PATH_DEP}'",
        ],
        f"CREATE FUNCTION {CONTROL_PATH_EXT}() RETURNS int LANGUAGE sql AS 'SELECT 2';",
    )

    # This one is never created, we only check that pg_extension_base finds
    # its preload request when it scans the directories in the path.
    write_extension(
        extension_directory,
        CONTROL_PATH_PRELOAD,
        [
            "comment = 'preloading extension installed via extension_control_path'",
            "default_version = '1.0'",
            "relocatable = true",
            f"#!shared_preload_libraries = '$libdir/{CONTROL_PATH_PRELOAD}'",
        ],
    )

    return str(base_directory)


@pytest.fixture
def control_path_conn(superuser_conn):
    """
    Connection that rolls back after the test, which also undoes the
    extension_control_path setting.
    """
    if get_pg_version_num(superuser_conn) < 180000:
        pytest.skip("extension_control_path was added in PostgreSQL 18")

    yield superuser_conn

    superuser_conn.rollback()


# pg_extension_base intercepts every CREATE EXTENSION to install and update
# dependencies, so it has to look for control files in the same directories as
# PostgreSQL. Otherwise, extensions that are installed in a directory in
# extension_control_path cannot be created at all.
def test_create_extension_in_control_path(
    control_path_conn, extension_control_directory
):
    run_command(
        f"SET extension_control_path TO '{extension_control_directory}:$system'",
        control_path_conn,
    )

    # the dependency lives in the same directory and is created via CASCADE
    run_command(f"CREATE EXTENSION {CONTROL_PATH_EXT} CASCADE", control_path_conn)

    result = run_query(
        f"SELECT {CONTROL_PATH_EXT}() AS ext, {CONTROL_PATH_DEP}() AS dep",
        control_path_conn,
    )
    assert result[0]["ext"] == 2
    assert result[0]["dep"] == 1

    # ALTER EXTENSION .. UPDATE goes through the dependency handling as well,
    # which reads the requires line from the control file
    run_command(f"ALTER EXTENSION {CONTROL_PATH_EXT} UPDATE", control_path_conn)


# Extensions in the share directory keep working when the path has other
# directories in front of it.
def test_create_extension_in_share_directory(
    control_path_conn, extension_control_directory
):
    run_command(
        f"SET extension_control_path TO '{extension_control_directory}:$system'",
        control_path_conn,
    )

    run_command(
        "CREATE EXTENSION pg_extension_base_test_ext1 CASCADE", control_path_conn
    )

    result = run_query(
        "SELECT extversion FROM pg_extension WHERE extname = 'pg_extension_base'",
        control_path_conn,
    )
    assert len(result) == 1


# Without the directory in the path, the extension is not available, and the
# error should come from PostgreSQL rather than from pg_extension_base.
def test_create_extension_outside_control_path(
    control_path_conn, extension_control_directory
):
    error = run_command(
        f"CREATE EXTENSION {CONTROL_PATH_EXT} CASCADE",
        control_path_conn,
        raise_error=False,
    )
    assert f'extension "{CONTROL_PATH_EXT}" is not available' in error

    control_path_conn.rollback()

    # nothing was created
    result = run_query(
        f"SELECT 1 FROM pg_extension WHERE extname = '{CONTROL_PATH_DEP}'",
        control_path_conn,
    )
    assert len(result) == 0


# The libraries that extensions ask to preload are collected from all the
# directories in the path.
def test_list_preload_libraries_in_control_path(
    control_path_conn, extension_control_directory, pg_extension_base
):
    result = run_query(
        f"SELECT library_name FROM extension_base.list_preload_libraries()"
        f" WHERE extension_name = '{CONTROL_PATH_PRELOAD}'",
        control_path_conn,
    )
    assert len(result) == 0

    run_command(
        f"SET extension_control_path TO '{extension_control_directory}:$system'",
        control_path_conn,
    )

    result = run_query(
        f"SELECT library_name FROM extension_base.list_preload_libraries()"
        f" WHERE extension_name = '{CONTROL_PATH_PRELOAD}'",
        control_path_conn,
    )
    assert len(result) == 1
    assert result[0]["library_name"] == f"$libdir/{CONTROL_PATH_PRELOAD}"
