TEST_SOURCES = $(wildcard *.c)
TEST_OBJECTS = $(TEST_SOURCES:.c=.o)
TEST_EXEC = prepared_statements


PG_CONFIG ?= pg_config

# Get LIBDIR INCLUDE_DIR from pg_config
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)
PG_INCLUDEDIR = $(shell $(PG_CONFIG) --includedir)

# PGXS: Locates PostgreSQL extension building infrastructure using 'pg_config'.
PGXS := $(shell $(PG_CONFIG) --pgxs)

# compile with C11 option to use modern C
PG_CPPFLAGS = -std=c11 -I$(PG_INCLUDEDIR) -g

PG_LDFLAGS  = -L$(PG_LIBDIR) -lpq -lpgcommon -lduckdb -Wl,-rpath,$(PG_LIBDIR)

include ../../../shared.mk

# Rule to compile the test code
$(TEST_EXEC): $(TEST_OBJECTS)
	$(CC) -o $@ $^ $(PG_CPPFLAGS) $(PG_LDFLAGS)

clean:
	rm -f $(TEST_OBJECTS) $(TEST_EXEC)

USE_PGXS=1
include $(PGXS)


# Custom target for running Python tests
.PHONY: check
check: $(TEST_EXEC)
	AWS_DEFAULT_REGION=ca-west-1 AWS_ACCESS_KEY_ID=notreals3key AWS_SECRET_ACCESS_KEY=notrealskey ./$(TEST_EXEC)
	@if [ $$? -eq 0 ]; then \
		echo "pgduck_server C tests passed successfully!"; \
	else \
		echo "pgduck_server C tests failed!"; \
		exit 1; \
	fi
