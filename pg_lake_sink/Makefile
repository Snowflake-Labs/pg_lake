# pg_lake_sink is a Rust/pgrx extension, so this Makefile bridges the pg_lake build to cargo
# rather than using PGXS. It is an opt-in target: the top-level Makefile does not build it as part
# of `all`/`install`, since it pulls in Rust + cmake + a vendored librdkafka.
#
# Requires: a Rust toolchain, `cargo-pgrx` matching the pgrx dependency, and (on this box) a
# libclang the system glibc can dlopen — set LIBCLANG_PATH=/usr/lib64. See README.md.

PG_CONFIG ?= pg_config
CARGO ?= cargo

all:
	$(CARGO) build --release

install:
	$(CARGO) pgrx install --release --pg-config $(PG_CONFIG)

clean:
	$(CARGO) clean

# Python integration tests (pytest), sharing the repo-wide test harness in ../test_common.
# They need the extension installed AND a Kafka broker (docker/docker-compose.yml); tests
# skip cleanly when either is missing, so this is safe to run even without a Rust build.
check:
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

# Same tests against an already-running server (see the repo README / CLAUDE.md installcheck flow).
installcheck:
	PGDUCK_PORT=5332 PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck

uninstall:
	@echo "pg_lake_sink: remove \$$libdir/pg_lake_sink.so and the extension files manually"

.PHONY: all install clean check installcheck uninstall
