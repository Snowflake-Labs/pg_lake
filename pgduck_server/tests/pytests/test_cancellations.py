import psycopg2
import pytest
import threading
import time
from utils_pytest import *


def test_query_cancellation(pgduck_server):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )

    # Long-running query
    long_running_query = (
        "select sum(generate_series) FROM generate_series(0,999999999999)"
    )
    run_query_and_cancel(conn, long_running_query)

    conn.rollback()

    # Also run with transmit
    run_query_and_cancel(conn, "transmit " + long_running_query)

    # now, we should be able to re-connect
    run_simple_command(server_params.PGDUCK_UNIX_DOMAIN_PATH, server_params.PGDUCK_PORT)

    conn.close()


def run_query_and_cancel(conn, long_running_query):
    # Use a list to share data between threads, as thread-safe for simple operations
    exception_info = []

    # This function will run in a separate thread to allow us to cancel the query
    def execute_long_running_query():
        try:
            # Attempt to execute the long-running query
            cur = conn.cursor()
            cur.execute(long_running_query)
        except Exception as e:
            # Store exception info to check in main thread
            exception_info.append(str(e))

    # Start the long-running query in a thread
    query_thread = threading.Thread(target=execute_long_running_query)
    query_thread.start()

    # give a little bit time for the query to actually start running
    time.sleep(0.1)

    # Now, cancel the query
    conn.cancel()

    # Wait for the thread to finish
    query_thread.join()

    # Check if exception was raised and matches expected
    if exception_info:  # List is not empty, meaning exception was caught
        assert (
            "INTERRUPT Error: Interrupted" in exception_info[0]
        ), "Query cancellation did not raise the expected exception."
    else:
        pytest.fail(
            "No exception caught from cancelled query, expected cancellation exception."
        )


# test for small number of clients cancelled in a loop
def test_query_cancellation_for_multiple_clients_in_loop(
    num_iterations=10, num_clients=2
):
    for _ in range(num_iterations):
        test_query_cancellation_for_multiple_clients(num_clients)


# test for many clients canceled at once
def test_query_cancellation_for_multiple_clients(pgduck_server, num_clients=20):

    # Define the long-running query
    long_running_query = (
        "SELECT SUM(generate_series) FROM generate_series(0,999999999999)"
    )

    # Function to execute the long-running query for each client
    def execute_long_running_query(conn, exception_info):
        try:
            cur = conn.cursor()
            cur.execute(long_running_query)
        except Exception as e:
            # Store exception info to check in the main thread
            exception_info.append(str(e))

    # Use a list to store exception info for each client
    exception_infos = []

    # Create and start threads for each client
    threads = []
    for _ in range(num_clients):
        conn = psycopg2.connect(
            host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
        )
        exception_infos.append([])  # Initialize exception info for this client
        thread = threading.Thread(
            target=execute_long_running_query, args=(conn, exception_infos[-1])
        )
        thread.start()
        threads.append((conn, thread))

    # give a little bit time for the query to actually start running
    time.sleep(0.1)

    # Cancel queries for each client
    for conn, _ in threads:
        conn.cancel()
        conn.rollback()

        # make sure we can still run queries successfully
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchall() == [("1",)]

    # Wait for all threads to finish
    for _, thread in threads:
        thread.join()

    # make sure all clients cancelled
    assert len(exception_infos) == num_clients

    # Check exceptions for each client
    for i, exception_info in enumerate(exception_infos):
        if exception_info:
            assert (
                "INTERRUPT Error: Interrupted" in exception_info[0]
            ), f"Query cancellation for client {i+1} did not raise the expected exception."
        else:
            pytest.fail(
                f"No exception caught from cancelled query for client {i+1}, expected cancellation exception."
            )

    # Cleanup
    for conn, _ in threads:
        conn.close()
