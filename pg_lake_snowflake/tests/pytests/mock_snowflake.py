"""A mock of the Snowflake SQL API v2, enough to test the wrapper without an account.

The mock records every statement it is asked to run and answers from a route
table, which lets a test assert on the SQL that was pushed down and on how the
wrapper turns a result set back into rows. It speaks the parts of the API the
wrapper uses: submitting a statement, answering 202 and being polled, serving
result partitions one at a time, cancelling, and failing with a Snowflake error
body.
"""

import gzip
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

# the value the API reports as the length of an unbounded VARCHAR
UNBOUNDED_TEXT_LENGTH = 16777216


def column(name, type_name, precision=None, scale=None, length=None, nullable=True):
    """One entry of resultSetMetaData.rowType."""
    return {
        "name": name,
        "type": type_name,
        "precision": precision,
        "scale": scale,
        "length": length,
        "nullable": nullable,
        "database": "MOCK",
        "schema": "MOCK",
        "table": "MOCK",
        "byteLength": None,
        "collation": None,
    }


class Route:
    """What the mock answers for statements containing a given substring."""

    def __init__(self, columns, partitions, status=200, error=None, asynchronous=False):
        self.columns = columns
        self.partitions = partitions
        self.status = status
        self.error = error
        self.asynchronous = asynchronous
        self.poll_count = 0


class MockSnowflake:
    def __init__(self):
        self.statements = []
        self.cancelled = []
        self.requests_without_authorization = 0
        self.compressed_responses = 0
        self.routes = []
        self.default_route = Route([column("STATUS", "text")], [[["ok"]]])
        self._handles = {}
        self._next_handle = 1
        self._server = None
        self._thread = None

    # -- setup ------------------------------------------------------------

    def route(
        self, substring, columns, partitions, status=200, error=None, asynchronous=False
    ):
        """Answer statements containing substring with the given result.

        The most recently added route wins, so a test can add a general route for
        a table and then a more specific one for the statement it cares about.
        """
        route = Route(columns, partitions, status, error, asynchronous)
        self.routes.insert(0, (substring, route))
        return route

    def start(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]

        mock = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):
                pass

            def _respond(self, status, payload, compress=False):
                body = json.dumps(payload).encode()

                if compress:
                    body = gzip.compress(body)
                    mock.compressed_responses += 1

                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                if compress:
                    self.send_header("Content-Encoding", "gzip")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _read_body(self):
                length = int(self.headers.get("Content-Length") or 0)
                if length == 0:
                    return {}
                return json.loads(self.rfile.read(length))

            def _check_authorization(self):
                if not self.headers.get("Authorization"):
                    mock.requests_without_authorization += 1

            def do_POST(self):
                self._check_authorization()
                path = urlparse(self.path).path

                if path.endswith("/cancel"):
                    handle = path.split("/")[-2]
                    mock.cancelled.append(handle)
                    self._respond(200, {"message": "SQL execution canceled"})
                    return

                body = self._read_body()
                statement = body.get("statement", "")
                mock.statements.append(statement)

                route = mock.route_for(statement)
                handle = mock.register(route)

                if route.error is not None:
                    self._respond(
                        route.status, dict(route.error, statementHandle=handle)
                    )
                    return

                if route.asynchronous:
                    self._respond(
                        202,
                        {
                            "statementHandle": handle,
                            "statementStatusUrl": f"/api/v2/statements/{handle}",
                            "message": "Asynchronous execution in progress.",
                        },
                    )
                    return

                acceptsGzip = "gzip" in (self.headers.get("Accept-Encoding") or "")

                self._respond(
                    200, mock.result_payload(route, handle, 0), compress=acceptsGzip
                )

            def do_GET(self):
                self._check_authorization()
                parsed = urlparse(self.path)
                handle = parsed.path.split("/")[-1]
                route = mock._handles.get(handle)

                if route is None:
                    self._respond(404, {"code": "000000", "message": "unknown handle"})
                    return

                partition = int(parse_qs(parsed.query).get("partition", ["0"])[0])

                if route.asynchronous and route.poll_count == 0:
                    route.poll_count += 1
                    self._respond(
                        202,
                        {
                            "statementHandle": handle,
                            "statementStatusUrl": f"/api/v2/statements/{handle}",
                            "message": "Asynchronous execution in progress.",
                        },
                    )
                    return

                #
                # Snowflake returns a result partition gzipped whether or not the
                # request said it could take it that way, which is exactly the
                # case that broke the wrapper once.
                #
                self._respond(
                    200,
                    mock.result_payload(route, handle, partition),
                    compress=partition > 0,
                )

        self._server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()

        return f"http://127.0.0.1:{port}"

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None

    # -- request handling -------------------------------------------------

    def route_for(self, statement):
        for substring, route in self.routes:
            if substring in statement:
                return route
        return self.default_route

    def register(self, route):
        handle = f"mock-{self._next_handle:04d}"
        self._next_handle += 1
        self._handles[handle] = route
        return handle

    def result_payload(self, route, handle, partition):
        rows = route.partitions[partition] if partition < len(route.partitions) else []

        return {
            "resultSetMetaData": {
                "numRows": sum(len(part) for part in route.partitions),
                "format": "jsonv2",
                "partitionInfo": [{"rowCount": len(part)} for part in route.partitions],
                "rowType": route.columns,
            },
            "data": rows,
            "code": "090001",
            "sqlState": "00000",
            "statementHandle": handle,
            "message": "Statement executed successfully.",
        }

    # -- assertions -------------------------------------------------------

    def last_statement(self):
        assert self.statements, "no statement reached the mock"
        return self.statements[-1]

    def statement_containing(self, substring):
        matches = [statement for statement in self.statements if substring in statement]
        assert matches, f"no statement contained {substring!r}: {self.statements}"
        return matches[-1]
