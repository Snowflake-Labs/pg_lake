from utils_pytest import *


def test_psql_simple_select(pgduck_server):

    file_path = "/tmp/test_simple_1.sql"
    file = open(file_path, "w")
    file.write("SELECT 1;\n")
    file.close()

    for i in range(0, 5):
        returncode, stdout, stderr = run_psql_command(
            [
                "-c",
                "SELECT " + str(i),
                file_path,
                "-h",
                server_params.PGDUCK_UNIX_DOMAIN_PATH,
                "-p",
                str(server_params.PGDUCK_PORT),
            ]
        )

        assert returncode == 0, f"psql prepared has not returned expected error code"
        assert (
            str(i) + "\n(1 row)\n" in stdout
        ), f"pgbench prepared has not failed with expected error message"

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)
