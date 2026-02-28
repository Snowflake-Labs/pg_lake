EXTENSION = pg_extension_base

MODULE_big = $(EXTENSION)

DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--1.6.sql
SOURCES := $(wildcard *.c) $(wildcard */*.c)
OBJS := $(patsubst %.c,%.o,$(sort $(SOURCES)))

HEADERS = $(wildcard include/pg_extension_base/*.h)

PG_CPPFLAGS = -Iinclude -std=gnu99 -Wall -Wextra -Wno-unused-parameter
override CFLAGS := $(filter-out -Wdeclaration-after-statement,$(CFLAGS))

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
USE_PGXS=1
include $(PGXS)

SHAREDIR := $(shell $(PG_CONFIG) --sharedir)

pg_extension_base-test:
	$(MAKE) -C tests/pg_extension_base_test_common/
	$(MAKE) -C tests/pg_extension_base_test_ext1/
	$(MAKE) -C tests/pg_extension_base_test_ext2/
	$(MAKE) -C tests/pg_extension_base_test_ext3/
	$(MAKE) -C tests/pg_extension_base_test_scheduler/
	$(MAKE) -C tests/pg_extension_base_test_hibernate/

install-pg_extension_base-test: pg_extension_base-test
	$(MAKE) -C tests/pg_extension_base_test_common/ install
	$(MAKE) -C tests/pg_extension_base_test_ext1/ install
	$(MAKE) -C tests/pg_extension_base_test_ext2/ install
	$(MAKE) -C tests/pg_extension_base_test_ext3/ install
	$(MAKE) -C tests/pg_extension_base_test_scheduler/ install
	$(MAKE) -C tests/pg_extension_base_test_hibernate/ install

uninstall-pg_extension_base-test:
	$(MAKE) -C tests/pg_extension_base_test_common/ uninstall
	$(MAKE) -C tests/pg_extension_base_test_ext1/ uninstall
	$(MAKE) -C tests/pg_extension_base_test_ext2/ uninstall
	$(MAKE) -C tests/pg_extension_base_test_ext3/ uninstall
	$(MAKE) -C tests/pg_extension_base_test_scheduler/ uninstall
	$(MAKE) -C tests/pg_extension_base_test_hibernate/ uninstall

clean-pg_extension_base-test:
	$(MAKE) -C tests/pg_extension_base_test_common/ clean
	$(MAKE) -C tests/pg_extension_base_test_ext1/ clean
	$(MAKE) -C tests/pg_extension_base_test_ext2/ clean
	$(MAKE) -C tests/pg_extension_base_test_ext3/ clean
	$(MAKE) -C tests/pg_extension_base_test_scheduler/ clean
	$(MAKE) -C tests/pg_extension_base_test_hibernate/ clean

prepare-check: install install-pg_extension_base-test
finish-check: uninstall-pg_extension_base-test

# Custom target for running Python tests
# Run prepare-check before check (might require superuser)
check:
	if [ ! -f "$(SHAREDIR)/extension/pg_extension_base_test_ext1.control" ] ; \
	then \
		echo pg_extension_base_test_ext1 is not installed, run make prepare-check first ; \
		exit 1 ; \
	fi
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests

clean: clean-pg_extension_base-test
uninstall: uninstall-pg_extension_base-test

.PHONY: installcheck
installcheck:
	if [ ! -f "$(SHAREDIR)/extension/pg_extension_base_test_ext1.control" ] ; \
	then \
		echo pg_extension_base_test_ext1 is not installed, run make prepare-check first ; \
		exit 1 ; \
	fi
	@echo "Running Python tests..."
	PYTHONPATH=../test_common pipenv run pytest -v tests/pytests --installcheck
