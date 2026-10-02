PG_CONFIG ?= pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_LIBDIR := $(shell $(PG_CONFIG) --libdir)
PG_BINDIR := $(shell $(PG_CONFIG) --bindir)
PG_SHAREDIR := $(shell $(PG_CONFIG) --sharedir)

USE_PGXS=1
include $(PGXS)

JAR_VERSION=1.8.0

# Number of attempts at downloading the Gradle distribution before giving up.
GRADLE_BOOTSTRAP_TRIES ?= 5

# Polaris build and install
all: gradle_distribution
	cd polaris && ./gradlew :polaris-server:assemble :polaris-server:quarkusAppPartsBuild :polaris-admin:assemble :polaris-relational-jdbc:assemble -Dquarkus.container-image.build=false -Dquarkus.package.jar.type=uber-jar --rerun

# Install the Gradle distribution the wrapper needs, on our own retry.
#
# The wrapper downloads it with a 10-second timeout and a single attempt, and
# reports a slow mirror as a build failure:
#
#   Attempt 1/1 failed. Reason: Downloading from
#   https://services.gradle.org/distributions/gradle-9.6.1-all.zip failed:
#   timeout (10000ms)
#   Exception in thread "main" java.io.IOException: ...
#
# That timeout lives in polaris/gradle/wrapper/gradle-wrapper.properties, which
# belongs to the Polaris submodule and is not ours to raise. `gradlew --version`
# installs the distribution and runs nothing else, so doing it first -- and
# retrying it -- leaves the build above to find the distribution in the wrapper
# cache and fail only for its own reasons.
.PHONY: gradle_distribution
gradle_distribution:
	cd polaris && i=1 && while [ $$i -le $(GRADLE_BOOTSTRAP_TRIES) ]; do \
		./gradlew --version >/dev/null && exit 0; \
		echo "Gradle distribution not installed (attempt $$i/$(GRADLE_BOOTSTRAP_TRIES)); retrying in 5s" >&2; \
		sleep 5; \
		i=$$((i + 1)); \
	done; \
	echo "could not install the Gradle distribution after $(GRADLE_BOOTSTRAP_TRIES) attempts" >&2; \
	exit 1

install:
	cp polaris/gradlew $(PG_BINDIR)/. && \
	cp polaris/runtime/admin/build/polaris-admin-$(JAR_VERSION)-runner.jar $(PG_BINDIR)/polaris-admin.jar && \
	cp polaris/runtime/server/build/polaris-server-$(JAR_VERSION)-runner.jar $(PG_BINDIR)/polaris-server.jar

clean:
	cd polaris && ./gradlew clean

