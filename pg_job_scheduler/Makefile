EXTENSION = pg_job_scheduler

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--1.0.sql
SOURCES := $(wildcard *.c) $(wildcard */*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

PG_CPPFLAGS = -Iinclude -I../pg_extension_base/include -std=gnu99 -Wall -Wextra -Wno-unused-parameter
override CFLAGS := $(filter-out -Wdeclaration-after-statement,$(CFLAGS))

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHLIB_LINK += -undefined dynamic_lookup
endif

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

include ../shared.mk

SHAREDIR := $(shell $(PG_CONFIG) --sharedir)

prepare-check: install
finish-check:

check:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

.PHONY: installcheck
installcheck:
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck
