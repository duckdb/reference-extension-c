.PHONY: clean clean_all

PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Main extension configuration
EXTENSION_NAME=reference_extension_c
MINIMUM_DUCKDB_VERSION_MAJOR=0
MINIMUM_DUCKDB_VERSION_MINOR=0
MINIMUM_DUCKDB_VERSION_PATCH=1
MINIMUM_DUCKDB_VERSION=v$(MINIMUM_DUCKDB_VERSION_MAJOR).$(MINIMUM_DUCKDB_VERSION_MINOR).$(MINIMUM_DUCKDB_VERSION_PATCH)

all: configure release

# Include makefiles from DuckDB
include extension-ci-tools/makefiles/c_api_extensions/base.Makefile
include extension-ci-tools/makefiles/c_api_extensions/c_cpp.Makefile

configure: venv platform extension_version

debug: build_extension_library_debug build_extension_with_metadata_debug
release: build_extension_library_release build_extension_with_metadata_release

test: test_debug
test_debug: test_extension_debug
test_release: test_extension_release

clean: clean_build clean_cmake
clean_all: clean_configure clean