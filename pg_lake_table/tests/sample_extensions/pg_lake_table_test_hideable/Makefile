EXTENSION = pg_lake_table_test_hideable

MODULE_big = $(EXTENSION)

DATA = $(EXTENSION)--1.0.sql
SOURCES := $(wildcard src/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

PG_CPPFLAGS = -Iinclude -I../../../include -I../../../../pg_extension_base/include -std=gnu11
override CFLAGS := $(filter-out -Wdeclaration-after-statement,$(CFLAGS))

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)
