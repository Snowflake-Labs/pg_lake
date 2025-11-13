# when adding a new extension, add to this list to get the standard targets supported
EXT_TARGETS = pg_extension_base pg_map pg_extension_updater pg_lake_engine pg_lake_copy pg_lake_table pg_lake_iceberg pg_lake_spatial pg_lake pg_lake_benchmark
DUCK_TARGETS = pgduck_server duckdb_pglake
ALL_TARGETS = $(DUCK_TARGETS) avro $(EXT_TARGETS)

# generated phony targets
ACTION_LIST = clean install uninstall check installcheck
PHONY_TARGETS = $(foreach target, $(ALL_TARGETS), $(target) $(foreach action, $(ACTION_LIST), $(action)-$(target))) installcheck-postgres installcheck-postgres-with_extensions_created

# if you want to override a target's specific implementation from the default, you must add it to this list
CUSTOM_TARGETS = check-pg_lake_engine installcheck-pg_lake_engine check-pg_extension_updater installcheck-pg_extension_updater install-avro check-avro clean-avro uninstall-avro check-duckdb_pglake

# other phony targets go here
.PHONY: all install installcheck clean check submodules fast install-fast uninstall check-indent reindent ci-cached install-ci-cached clean-ci-cached
.PHONY: cmake-avro
.PHONY: $(ALL_TARGETS)
.PHONY: $(PHONY_TARGETS)

# top-level targets defined in terms of our variables
all: submodules $(ALL_TARGETS)
install: $(addprefix install-,$(ALL_TARGETS))
clean: $(addprefix clean-,$(ALL_TARGETS))
check-local: $(addprefix check-,$(ALL_TARGETS))
check-upgrade: check-pg_lake_table-upgrade
check-e2e: check-pg_lake_table-e2e
check: check-local check-e2e
installcheck: installcheck-local installcheck-e2e
installcheck-local: installcheck-postgres installcheck-postgres-with_extensions_created installcheck-pgduck_server $(addprefix installcheck-,$(EXT_TARGETS))
installcheck-e2e: installcheck-pg_lake_table-e2e
uninstall: $(addprefix uninstall-,$(ALL_TARGETS))

# variables needed for additional targets
PG_CONFIG ?= pg_config
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)
PG_MAJOR_VERSION := $(shell $(PG_CONFIG) --version | cut -f2 -d' ' | cut -f 1 -d.)

# Detect operating system
UNAME_S := $(shell uname -s)

# List of targets for indent checks
INDENT_TARGETS = pgduck_server $(EXT_TARGETS)
TYPEDEFS = /tmp/typedefs-$(PG_MAJOR_VERSION).list

CMAKE_AVRO_ARGS = -DCMAKE_INSTALL_PREFIX=avrolib -DCMAKE_BUILD_TYPE=RelWithDebInfo 

# Conditionally set the library name
ifeq ($(UNAME_S),Linux)
    LIB_NAME = libduckdb.so
endif
ifeq ($(UNAME_S),Darwin)
    LIB_NAME = libduckdb.dylib
    CMAKE_AVRO_ARGS += -DSNAPPY_INCLUDE_DIRS=/opt/homebrew/opt/snappy/include/
endif

fast:
	cp $(PG_LIBDIR)/$(LIB_NAME) duckdb_pglake/$(LIB_NAME)
	$(foreach extension, pgduck_server $(EXT_TARGETS), $(MAKE) -j 16 -C $(extension); )

install-fast: fast
	$(foreach target, $(ALL_TARGETS), $(MAKE) -C $(target) install; )

# Steps to avoid (re)building duckdb_pglake and avro -- XXX should these be EXT_TARGETS?
ci-cached: pgduck_server_no_deps pg_extension_base pg_map pg_extension_updater pg_lake_copy pg_lake_iceberg pg_lake_table pg_lake_spatial pg_lake_benchmark
install-ci-cached: install-pgduck_server_no_deps install-duckdb_pglake install-pg_extension_base install-pg_map install-pg_extension_updater install-pg_lake_engine install-pg_lake_copy install-pg_lake_table install-pg_lake_iceberg install-pg_lake_spatial install-pg_lake install-pg_lake_benchmark
clean-ci-cached: clean-pgduck_server clean-pg_extension_base clean-pg_map clean-pg_extension_updater clean-pg_lake_engine clean-pg_lake_copy clean-pg_lake_iceberg clean-pg_lake_spatial clean-pg_lake_table clean-pg_lake clean-pg_lake_benchmark

# style/indent-related changes

# This target ensures that we download the latest major version's typedefs.list
# from buildfarm if we don't have a local copy.  Since we currently do not
# override the typedefs.list, this should be equivalent to what we were
# previously doing by pulling from the postgres source tree.

typedefs: $(TYPEDEFS)

$(TYPEDEFS):
	curl -o $(TYPEDEFS) https://buildfarm.postgresql.org/cgi-bin/typedefs.pl?branch=REL_$(PG_MAJOR_VERSION)_STABLE

check-indent: typedefs
	for dir in $(INDENT_TARGETS); do \
		pgindent --typedefs=$(TYPEDEFS) $(INDENT_TARGETS); \
	done
	pipenv run black --check --diff $(INDENT_TARGETS)

reindent: typedefs
	pgindent --typedefs=$(TYPEDEFS) $(INDENT_TARGETS)
	pipenv run black $(INDENT_TARGETS)

submodules:
	git submodule init
	git submodule update

## module declarations; each extension should have its dependencies spelled out here
pg_map:
	$(MAKE) -C pg_map

pg_extension_updater:
	$(MAKE) -C pg_extension_updater

pg_extension_base: pg_map pg_extension_updater
	$(MAKE) -C pg_extension_base

pg_lake_engine: pg_extension_base avro install-avro-local
	$(MAKE) -C pg_lake_engine

pg_lake_table: pg_lake_engine pg_lake_iceberg install-avro-local
	$(MAKE) -C pg_lake_table

pg_lake_copy: pg_lake_engine
	$(MAKE) -C pg_lake_copy

pg_lake_iceberg: pg_lake_engine install-avro-local
	$(MAKE) -C pg_lake_iceberg

pg_lake_spatial: pg_lake_engine
	$(MAKE) -C pg_lake_spatial

pg_lake: pg_lake_table
	$(MAKE) -C pg_lake

pgduck_server: duckdb_pglake
	$(MAKE) -C pgduck_server

duckdb_pglake: submodules
	$(MAKE) -C duckdb_pglake

## Overridden targets; basically the ones in CUSTOM_TARGETS above
check-pg_lake_engine:
	# Currently we expect extensions to implement end-to-end-tests

installcheck-pg_lake_engine:
	# noop since check is also not supported, but needed to prevent default match

check-pg_extension_updater:
	# no tests yet

installcheck-pg_extension_updater:
	# no tests yet

check-duckdb_pglake:
	# noop


# other avro stuff managed here
cmake-avro: submodules
	mkdir -p avro/lang/c/build
	cd avro/lang/c/build && cmake .. $(CMAKE_AVRO_ARGS)

avro: cmake-avro
	cd avro && (patch -l -p1 -N < ../avro.patch || [ $$? -eq 1 ]; )
	$(MAKE) -C avro/lang/c/build

install-avro-local: avro
	# we install the library into a local directory and link statically against avro
	$(MAKE) -C avro/lang/c/build install

install-avro: install-avro-local
	install avro/lang/c/build/avrolib/lib*/libavro.* $(DESTDIR)$(PG_LIBDIR)

check-avro: avro
	$(MAKE) -C avro/lang/c/build test

clean-avro:
ifneq ("$(wildcard avro/lang/c/build)","")
	make -C avro/lang/c/build clean
	rm -r avro/lang/c/build
endif

uninstall-avro:
	rm -f $(PG_LIBDIR)/libavro.*

## Other targets
check-isolation_pg_lake_table:
	$(MAKE) -C pg_lake_table check-isolation

check-pg_lake_table-e2e:
	$(MAKE) -C pg_lake_table check-e2e

check-pg_lake_table-upgrade:
	$(MAKE) -C pg_lake_table check-upgrade

installcheck-pg_lake_table-e2e:
	$(MAKE) -C pg_lake_table installcheck-e2e

installcheck-postgres:
	$(PG_REGRESS_DIR)/pg_regress --host localhost --inputdir=$(PG_REGRESS_DIR) --outputdir=$(PG_REGRESS_DIR) --expecteddir=$(PG_REGRESS_DIR) --schedule=$(PG_REGRESS_DIR)/parallel_schedule --dlpath=$(PG_REGRESS_DIR)

installcheck-postgres-with_extensions_created:
	$(PG_REGRESS_DIR)/pg_regress --host localhost --inputdir=$(PG_REGRESS_DIR) --outputdir=$(PG_REGRESS_DIR) --expecteddir=$(PG_REGRESS_DIR) --schedule=$(PG_REGRESS_DIR)/parallel_schedule --dlpath=$(PG_REGRESS_DIR) --load-extension="pg_map" --load-extension="pg_extension_base" --load-extension="pg_lake_engine" --load-extension="pg_lake_iceberg" --load-extension="btree_gist" --load-extension="pg_lake_table" --load-extension="pg_lake_copy" --load-extension="pg_lake" --load-extension="pg_lake_benchmark"
# --load-extension="postgis" --load-extension="pg_lake_spatial"

# do not build dependencies, used in the CI with caching
# we do depend on submodules to use duckdb.h
pgduck_server_no_deps: submodules
	$(MAKE) -C pgduck_server

install-pgduck_server_no_deps: pgduck_server_no_deps
	$(MAKE) -C pgduck_server install

# sub-directories follows this version, so when updating
# make sure to update them as well
PG_BINDIR := $(shell $(PG_CONFIG) --bindir)
REST_INSTALL_FILE := $(PG_BINDIR)/polaris-admin.jar

# install only rest catalog when the file does not exist
$(REST_INSTALL_FILE): rest_submodule
	$(MAKE) -C test_common/rest_catalog install

install-rest_catalog: $(REST_INSTALL_FILE)

rest_submodule:
	git submodule init test_common/rest_catalog/polaris
	git submodule update test_common/rest_catalog/polaris

## define a standard dispatch prefix for these targets; if you want to customize
## a generated one go ahead and add it to the CUSTOM_TARGETS list and it will be
## skipped here

$(foreach target, $(ALL_TARGETS), \
    $(foreach action, $(ACTION_LIST), \
		$(if $(filter $(action)-$(target), $(CUSTOM_TARGETS)), , \
			$(eval $(action)-$(target): ; $(MAKE) -C $(target) $(action)))))
