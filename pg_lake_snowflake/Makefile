EXTENSION = pg_lake_snowflake

MODULE_big = $(EXTENSION)

SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--3.5.sql

PGFILEDESC = "pg_lake_snowflake - foreign data wrapper for Snowflake tables"

PG_CPPFLAGS = -Iinclude -I../pg_extension_base/include -I../pg_lake_engine/include -I../pg_lake_iceberg/include

# libcurl for the SQL API requests, libcrypto to sign key-pair JWTs
PG_LDFLAGS = -lcurl -lcrypto

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHLIB_LINK += -undefined dynamic_lookup
endif

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

include ../shared.mk

# Custom target for running Python tests
.PHONY: check
check:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

.PHONY: installcheck
installcheck:
	@echo "Running Python tests..."
	PGDUCK_PORT=5332 PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck

# Separate target for tests that talk to a real Snowflake account
.PHONY: check-e2e
check-e2e:
	@echo "Running Python end-to-end tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/e2e/
