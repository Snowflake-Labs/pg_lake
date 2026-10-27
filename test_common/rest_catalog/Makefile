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

# Lakekeeper, a second REST catalog for tests (#739). It is a single native
# binary, so we install a pinned release instead of building it. GitHub
# publishes no checksums for these assets; the SHA-256 values below were
# computed from the v$(LAKEKEEPER_VERSION) release, so bump them together.
LAKEKEEPER_VERSION = 0.14.2
LAKEKEEPER_SHA256_x86_64-unknown-linux-gnu = e1eb2de75f95c1124f91ed32bfd14bcbddf906f91651bbd446ad9a0af99251b8
LAKEKEEPER_SHA256_aarch64-unknown-linux-gnu = 501b946d53189ea75c8921f4d6b03501214ee830ec45a9fe25e32c826d1c6889
LAKEKEEPER_SHA256_aarch64-apple-darwin = 427a06636f229841a4f5c13d30c4c41f160e426764c1fddaf7d8e89753211f07

UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)
ifeq ($(UNAME_S),Darwin)
LAKEKEEPER_TARGET = aarch64-apple-darwin
SHA256_CHECK = shasum -a 256 -c -
else ifeq ($(UNAME_M),aarch64)
LAKEKEEPER_TARGET = aarch64-unknown-linux-gnu
else
LAKEKEEPER_TARGET = x86_64-unknown-linux-gnu
endif
SHA256_CHECK ?= sha256sum -c -
LAKEKEEPER_TARBALL = lakekeeper-$(LAKEKEEPER_TARGET).tar.gz
LAKEKEEPER_URL = https://github.com/lakekeeper/lakekeeper/releases/download/v$(LAKEKEEPER_VERSION)/$(LAKEKEEPER_TARBALL)

# The release binary is unstripped (~190MB); strip it on Linux, where it
# travels in the CI build artifact.
.PHONY: install-lakekeeper
install-lakekeeper:
	tmp=$$(mktemp -d) && \
	curl -fsSL --retry 5 -o $$tmp/$(LAKEKEEPER_TARBALL) $(LAKEKEEPER_URL) && \
	echo "$(LAKEKEEPER_SHA256_$(LAKEKEEPER_TARGET))  $$tmp/$(LAKEKEEPER_TARBALL)" | $(SHA256_CHECK) && \
	tar -xzf $$tmp/$(LAKEKEEPER_TARBALL) -C $$tmp lakekeeper && \
	if [ "$(UNAME_S)" = Linux ]; then strip $$tmp/lakekeeper; fi && \
	install -m 755 $$tmp/lakekeeper $(PG_BINDIR)/lakekeeper && \
	rm -rf $$tmp

clean:
	cd polaris && ./gradlew clean

