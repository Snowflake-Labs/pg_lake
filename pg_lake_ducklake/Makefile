EXTENSION = pg_lake_ducklake
MODULE_big = $(EXTENSION)
SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))
DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--3.2.sql

PG_CONFIG ?= pg_config
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)

PG_CPPFLAGS = -I$(libpq_srcdir) -Iinclude -std=gnu99 -g -I../pg_lake_engine/include -I../pg_extension_base/include
SHLIB_LINK_INTERNAL = $(libpq)
PG_LDFLAGS = -L$(PG_LIBDIR) -Wl,-rpath,$(PG_LIBDIR)

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHLIB_LINK += -undefined dynamic_lookup
endif

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
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck
