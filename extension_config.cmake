# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(sshfs
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)

# Link into unittest/shell so tests can `require sshfs` (DuckDB v2 no longer links by default)
duckdb_extension_statically_link(sshfs)
