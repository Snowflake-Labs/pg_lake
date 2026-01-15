EXTENSION = pg_lake_engine

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--3.0.sql
SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))
SHLIB_LINK_INTERNAL = $(libpq)

PG_LAKE_DELTA_SUPPORT ?= 0

PG_CPPFLAGS = -I$(libpq_srcdir) -Iinclude -I../pg_extension_base/include -std=gnu99 -g -DPG_LAKE_DELTA_SUPPORT=$(PG_LAKE_DELTA_SUPPORT)

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
SHLIB_LINK += -undefined dynamic_lookup
endif

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

include ../shared.mk
