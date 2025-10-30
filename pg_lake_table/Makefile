EXTENSION = pg_lake_table

DATA = pg_lake_table--1.0.sql
MODULE_big = $(EXTENSION)

# Generated source files:
SOURCES := $(wildcard *.c) $(wildcard */*.c)
SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c) $(wildcard src/*/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--3.0.sql

PGFILEDESC = "pg_lake_table - foreign data wrapper for data lakes"

PG_CPPFLAGS = -I$(libpq_srcdir) -Iinclude -I../pg_extension_base/include -I../pg_lake_engine/include -I../pg_lake_iceberg/include
SHLIB_LINK_INTERNAL = $(libpq)

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHLIB_LINK += -undefined dynamic_lookup
endif

REGRESS = pg_lake_table

PG_CONFIG ?= pg_config

PG_ISOLATION_REGRESS_DIR := $(shell $(PG_CONFIG) --libdir)/pgxs/src/test/isolation

JDBC_DRIVER_PATH ?= /usr/share/java/postgresql.jar

PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

include ../shared.mk

$(EXTENSION)--1.0.sql: $(EXTENSION).sql
	cat $^ > $@


# Custom target for running Python tests
.PHONY: check
check:
	@echo "Running Python tests..."
	
	if [ -n "${CI_WORKER_COUNT}" ]; then \
		JDBC_DRIVER_PATH=$(JDBC_DRIVER_PATH) \
		PYTHONPATH=../test_common pipenv run pytest --splits=${CI_WORKER_COUNT} --group=${CI_WORKER_ID} -v tests/pytests; \
	else \
		JDBC_DRIVER_PATH=$(JDBC_DRIVER_PATH) \
		PYTHONPATH=../test_common pipenv run pytest -v tests/pytests; \
	fi

# Custom target for running isolation tests
# it is a bit more involved than the other tests
# we use pytest to setup s3 and pgduck_server
# and let isolation tester have its own db
.PHONY: check-isolation
check-isolation:
	@echo "Running isolation tests"
	export PG_REGRESS_SOCK_DIR=/tmp;
	PG_ISOLATION_REGRESS_DIR=$(PG_ISOLATION_REGRESS_DIR) \
	PYTHONPATH=../test_common pipenv run pytest -v tests/isolation/test_isolation.py --isolationtester || (cat ./tests/isolation/output/regression.diffs && exit 1)

# Separate target for tests that directly access remote storage
.PHONY: check-e2e
check-e2e:
	@echo "Running Python end-to-end tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/e2e/

# Separate target for tests that depend on pre-upgrade artifacts
.PHONY: check-upgrade
check-upgrade:
	@echo "Running Python upgrade tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/upgrade/

.PHONY: installcheck
installcheck:
	@echo "Running Python tests..."
	
	if [ -n "${CI_WORKER_COUNT}" ]; then \
		JDBC_DRIVER_PATH=$(JDBC_DRIVER_PATH) \
		PGDUCK_PORT=5332 PYTHONPATH=../test_common pipenv run pytest --splits=${CI_WORKER_COUNT} --group=${CI_WORKER_ID} -v tests/pytests --installcheck; \
	else \
		JDBC_DRIVER_PATH=$(JDBC_DRIVER_PATH) \
		PGDUCK_PORT=5332 PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck; \
	fi

# Separate target for tests that directly access remote storage
.PHONY: installcheck-e2e
installcheck-e2e:
	@echo "Running Python end-to-end tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/e2e/ --installcheck
