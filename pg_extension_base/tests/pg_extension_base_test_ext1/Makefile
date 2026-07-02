EXTENSION = pg_extension_base_test_ext1

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--1.0.sql
SOURCES := $(wildcard *.c) $(wildcard */*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

PG_CPPFLAGS = -Iinclude -std=gnu11
override CFLAGS := $(filter-out -Wdeclaration-after-statement,$(CFLAGS))

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)
