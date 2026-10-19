EXTENSION = pg_lake_cortex

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--3.5.sql

PGFILEDESC = "pg_lake_cortex - Snowflake Cortex AI functions"

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

include ../shared.mk

# Custom target for running Python tests
.PHONY: check
check:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common:../pg_lake_snowflake/tests/pytests pipenv run pytest -v tests/pytests

.PHONY: installcheck
installcheck:
	@echo "Running Python tests..."
	PGDUCK_PORT=5332 PYTHONPATH=../test_common:../pg_lake_snowflake/tests/pytests pipenv run pytest -v tests/pytests --installcheck

# Separate target for tests that talk to a real Snowflake account
.PHONY: check-e2e
check-e2e:
	@echo "Running Python end-to-end tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/e2e/
