PG_CONFIG ?= pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)
PG_BINDIR := $(shell $(PG_CONFIG) --bindir)
PG_SHAREDIR := $(shell $(PG_CONFIG) --sharedir)

USE_PGXS=1
include $(PGXS)

# change to trigger CI cache
# for Polaris version changes
JAR_VERSION=1.2.0

# Polaris build and install
all:
	cd polaris && ./gradlew :polaris-server:assemble :polaris-server:quarkusAppPartsBuild :polaris-admin:assemble :polaris-relational-jdbc:assemble -Dquarkus.container-image.build=false -Dquarkus.package.jar.type=uber-jar --rerun

install:
	cp polaris/gradlew $(PG_BINDIR)/. && \
	cp polaris/runtime/admin/build/polaris-admin-$(JAR_VERSION)-incubating-runner.jar $(PG_BINDIR)/polaris-admin.jar && \
	cp polaris/runtime/server/build/polaris-server-$(JAR_VERSION)-incubating-runner.jar $(PG_BINDIR)/polaris-server.jar

clean:
	cd polaris && ./gradlew clean

