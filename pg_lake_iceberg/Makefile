EXTENSION = pg_lake_iceberg
MODULE_big = $(EXTENSION)
SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c) $(wildcard src/*/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))
DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--3.0.sql

PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)

PG_CPPFLAGS = -I$(libpq_srcdir) -Iinclude -std=gnu99 -g -I../pg_lake_engine/include
SHLIB_LINK_INTERNAL = $(libpq)
PG_LDFLAGS  = -L$(PG_LIBDIR) -Wl,-rpath,$(PG_LIBDIR) -lavro -lcurl

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHLIB_LINK += -undefined dynamic_lookup
endif

JDBC_DRIVER_PATH ?= /usr/share/java/postgresql.jar

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

include ../shared.mk

PG_BINDIR       := $(shell $(PG_CONFIG) --bindir)
REST_INSTALL_FILE := $(PG_BINDIR)/polaris-admin.jar

$(EXTENSION)--1.2.sql: $(EXTENSION).sql
	cat $^ > $@

# Custom target for running Python tests
.PHONY: check

install-rest_catalog:
	@if [ ! -f "$(REST_INSTALL_FILE)" ]; then \
		$(MAKE) -C ../ install-rest_catalog; \
	else \
		echo "REST catalog already installed, skipping."; \
	fi

check: install-rest_catalog
	@echo "Running Python tests..."
	JDBC_DRIVER_PATH=$(JDBC_DRIVER_PATH) \
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

.PHONY: installcheck
installcheck:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck
