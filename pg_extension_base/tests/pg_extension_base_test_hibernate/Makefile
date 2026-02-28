EXTENSION = pg_extension_base_test_hibernate

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--1.0.sql
SOURCES := $(wildcard *.c) $(wildcard */*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

PG_CPPFLAGS = -Iinclude -std=gnu99 -I../../include
override CFLAGS := $(filter-out -Wdeclaration-after-statement,$(CFLAGS))

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHLIB_LINK += -undefined dynamic_lookup
endif

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)
