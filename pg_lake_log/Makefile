EXTENSION = pg_lake_log

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--1.0.sql
SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

SHLIB_LINK = $(libpq)

PG_CPPFLAGS = -I$(libpq_srcdir) \
              -Iinclude \
              -I../pg_extension_base/include \
              -I../pg_lake_engine/include \
              -I../pg_lake_iceberg/include \
              -I../pg_lake_table/include \
              -std=gnu99 -g -Wall -Wextra -Wno-unused-parameter -Werror

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHLIB_LINK += -undefined dynamic_lookup
endif

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

include ../shared.mk

.PHONY: check
check:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

.PHONY: installcheck
installcheck:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck
