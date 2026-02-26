"""Database connection helpers, query execution, and subprocess utilities."""

import subprocess
import threading
import queue

import psycopg2
import psycopg2.extras

from . import server_params


# ---------------------------------------------------------------------------
# Subprocess / process utilities
# ---------------------------------------------------------------------------

def terminate_process(proc, timeout=10):
    """Terminate a subprocess with timeout, falling back to SIGKILL.

    Sends SIGTERM first, then escalates to SIGKILL if the process does
    not exit within ``timeout`` seconds.  Safe to call on already-exited
    processes.
    """
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout)


def capture_output(file, output_queue):
    try:
        with file as pipe:
            for line in iter(pipe.readline, ""):
                output_queue.put(line)
                print(line, end="")
    except UnicodeDecodeError:
        # Server stderr may contain non-UTF-8 bytes; ignore and stop reading.
        pass
    finally:
        output_queue.put(None)


def get_server_output(output_queue):
    server_output = ""
    try:
        while True:
            line = output_queue.get(timeout=0.2)
            if line is not None:
                server_output += line
    except:
        # No more output in the queue
        pass

    return server_output


# ---------------------------------------------------------------------------
# PostgreSQL / psycopg2 connection helpers
# ---------------------------------------------------------------------------

def perform_query(query, conn):
    cur = conn.cursor()
    cur.execute(query)
    cur.close()


def default_connection_string(user=server_params.PG_USER):
    return f"dbname={server_params.PG_DATABASE} user={user} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"


def open_pg_conn(user=server_params.PG_USER):
    return psycopg2.connect(default_connection_string(user))


def open_pg_conn_to_db(dbname, user=server_params.PG_USER):
    conn_str = f"dbname={dbname} user={user} password={server_params.PG_PASSWORD} port={server_params.PG_PORT} host={server_params.PG_HOST}"
    return psycopg2.connect(conn_str)


def run_simple_command(hostname, serverport):

    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH, port=server_params.PGDUCK_PORT
    )
    cur = conn.cursor()
    cur.execute("SELECT 1")
    r = cur.fetchall()
    print(r)
    assert r == [("1",)]


def run_pgbench_command(commands):

    # for the purposes of these test, always use check_cli_params_only
    full_command = ["pgbench"] + commands
    process = subprocess.Popen(
        full_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


def run_psql_command(commands):

    # for the purposes of these test, always use check_cli_params_only
    full_command = ["psql", "-X", "-q"] + commands
    process = subprocess.Popen(
        full_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout, stderr = process.communicate()
    return process.returncode, stdout.decode(), stderr.decode()


def perform_query_on_cursor(query, conn):
    cur = conn.cursor()
    try:
        cur.execute(query)
        # Fetching all results
        results = cur.fetchall()
        return results
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    finally:
        cur.close()


def run_query(query, conn, raise_error=True):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    try:
        cur.execute(query)
        results = cur.fetchall()
        return results
    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        cur.close()


def run_command(query, conn, raise_error=True):
    cur = conn.cursor()
    try:
        cur.execute(query)
    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        cur.close()


def run_command_outside_tx(query_list, raise_error=True):
    conn = open_pg_conn()
    conn.autocommit = True

    cur = conn.cursor()

    try:
        for q in query_list:
            cur.execute(q)

    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        conn.close()



# Example usage for thread_run_query() and thread_run_command():
#   thread1 = thread_run_query("SELECT * FROM your_table", conn)
#   thread2 = thread_run_command("UPDATE your_table SET field='value'", conn)
#   thread1.join()  # Optionally wait for the thread to finish
#   thread2.join()  # Optionally wait for the thread to finish
def thread_run_query(query, conn, raise_error=True):
    thread = threading.Thread(target=run_query, args=(query, conn, raise_error))
    thread.start()
    return thread


def thread_run_command(query, conn, raise_error=True):
    thread = threading.Thread(target=run_command, args=(query, conn, raise_error))
    thread.start()
    return thread


def copy_to_file(copy_command, file_name, conn, raise_error=True):
    cursor = conn.cursor()
    try:
        with open(file_name, "wb") as file:
            cursor.copy_expert(copy_command, file)

        return None
    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        cursor.close()


def copy_from_file(copy_command, file_name, conn, raise_error=True):
    cursor = conn.cursor()
    try:
        with open(file_name, "rb") as file:
            cursor.copy_expert(copy_command, file)

        return None
    except psycopg2.DatabaseError as error:
        if raise_error:
            raise
        else:
            print(error.pgerror)
            return error.pgerror
    finally:
        cursor.close()


def get_pg_version_num(pg_conn) -> int:
    """
    Retrieves the server_version_num from PostgreSQL using the given connection.

    Args:
        pg_conn: The PostgreSQL connection object.

    Returns:
        int: The server version number as an integer.
    """
    query = "SHOW server_version_num;"
    result = run_query(query, pg_conn)

    if result and isinstance(result, list) and len(result) > 0:
        server_version_num = result[0][0]
        return int(server_version_num)

    raise ValueError("Server version number not found in the query result.")
