MODULE_big = pg_extension_base_test_common

SOURCES := $(wildcard src/*.c) $(wildcard src/*/*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

PG_CPPFLAGS = -Iinclude -std=gnu11 -g
override CFLAGS := $(filter-out -Wdeclaration-after-statement,$(CFLAGS))

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)
