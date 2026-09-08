# Rendering values into SQL

> Derived-from: Snowflake-Labs/pg_lake@031d6f58798d · generated 2026-09-03
> Regenerate when: a new helper renders a value into SQL text bound for DuckDB or for SPI; `duckdb_kwlist.h` stops being generated from the vendored keyword list; a call site starts interpolating a value into a query given to `run_attached`; or the FDW deparser starts binding parameters instead of interpolating constants.

pg_lake builds SQL as text in two directions, and both are injection surfaces:

- **outward to DuckDB**, over the socket, where the statement runs with the
  server's full privilege (`threat-model.md`, T1);
- **inward to PostgreSQL through SPI**, where several paths run as the extension
  owner, which is typically a superuser.

Neither direction uses bound parameters end to end, so the correctness of the
quoting helpers is what stands in for it. This document lists them.

## Outward: text sent to DuckDB

### Identifiers

`duckdb_quote_identifier` is pg_lake's replacement for core's
`quote_identifier`. It exists because DuckDB's parser is a fork of PostgreSQL's
with extra reserved words (`PIVOT`, `LAMBDA`, `QUALIFY`), so core's keyword list
under-quotes: an identifier that needs quoting in DuckDB and not in PostgreSQL
comes out bare.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/pgduck/keywords.c:74-134`, `tools/generate_duckdb_kwlist.py:1-21` @ 031d6f58798d (2026-09-03)

The keyword table is generated from the vendored DuckDB `kwlist.hpp` and
committed, with `make check-duckdb-kwlist` verifying it is not stale.

**Basis:** code-verified Snowflake-Labs/pg_lake `Makefile:229-238`, `.github/workflows/lint.yml:57-59` @ 031d6f58798d (2026-09-03)

**Bounded by:** the check proves the table matches the pinned DuckDB, not that
every call site uses it. An identifier rendered with core's `quote_identifier`
somewhere in the DuckDB-bound path would pass every gate in this repository.

### String literals

Three helpers, for three destinations:

- `deparseStringLiteral` for constants in deparsed FDW queries;
- `EscapeSingleQuotes` for values interpolated into `CREATE SECRET`, which is how
  vended credentials reach DuckDB (`data-and-secrets.md`);
- `QuoteDuckDBStructKey` for struct keys, which additionally escapes backslashes
  because the value has to survive a round trip through CSV.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_table/src/fdw/deparse.c:2520-2542`, `pg_lake_engine/src/pgduck/vended_secrets.c:78-102`, `pg_lake_engine/src/pgduck/struct_conversion.c:49-60,157` @ 031d6f58798d (2026-09-03)

### The deparser interpolates rather than binds

`deparseConst` renders constants into the query text instead of sending them as
parameters. That is a deliberate design choice for a pushdown deparser, and it
means every type's output function is part of the injection surface rather than
just the string case.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_table/src/fdw/deparse.c:2706-2790` @ 031d6f58798d (2026-09-03)

**Bounded by:** correctness here rests on each rendered constant being
re-parseable by DuckDB as the same value, which is a per-type property with no
single check behind it. A type whose output form DuckDB parses differently is a
correctness bug first and potentially an injection one second.

## Inward: text executed through SPI as the extension owner

Several bookkeeping paths run SQL as the `pg_lake_engine` extension owner via
`SPI_START_EXTENSION_OWNER`, because the caller has already had its permissions
checked and the internal tables are not user-writable. That makes those paths
the highest-value injection targets in the repository: the owner is usually a
superuser.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/cleanup/in_progress_files.c:281,357,426,530,755` @ 031d6f58798d (2026-09-03)

The in-progress-file insert shows the pattern that keeps this safe. The inner
statement is built as text with `quote_literal_cstr` around the path, and then
that whole statement is passed as a bound `$1` text parameter to
`extension_base.run_attached`:

```c
	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, TEXTOID, query->data, false);

	bool		readOnly = false;

	SPI_EXECUTE("select * from extension_base.run_attached($1)", readOnly);
```

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_engine/src/cleanup/in_progress_files.c:519-537`, `pg_extension_base/include/pg_extension_base/spi_helpers.h:54-90` @ 031d6f58798d (2026-09-03)

The reason the parameter matters is recorded in a regression test rather than in
the code: this call previously wrapped the inner query in `$$...$$`, and a `$$`
inside a user-supplied table `location` closed that quoting early. Since the
statement ran as the extension owner with `readOnly = false`, a `lake_write`
holder could append `ALTER ROLE ... SUPERUSER`. The test asserts the escalation
does not happen, and names the fix so a future change cannot quietly undo it.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_table/tests/pytests/test_security.py:79-155` @ 031d6f58798d (2026-09-03)

**Bounded by:** the structural lesson is that dollar-quoting a generated
statement is not safe when any part of it is user-influenced, and `location` is
user-influenced because `IsSupportedURL` only checks the scheme. Nothing
mechanically prevents a new `$$`-wrapped query from being introduced: the
protection is the convention of passing generated SQL through
`DECLARE_SPI_ARGS` / `SPI_ARG_VALUE`, plus the tests that cover the paths already
found. A new call site is not covered until someone writes the test.

Two more paths in the same class have their own regression tests, because the
user-influenced value arrives from further away in each:

- **the benchmark data generators.** `lake_tpch.gen` and `lake_tpcds.gen` take a
  `location`, and the tests cover both that the functions require `lake_write`
  and that a SQL-injection payload in `location` is blocked.

  **Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_benchmark/tests/pytests/test_security.py:1-4,40-207` @ 031d6f58798d (2026-09-03)

- **`catalog.json` for an object-store catalog.** The file is read from object
  storage, so its contents are attacker-influenced wherever a user can write to
  the catalog prefix (`threat-model.md`, T7). Three tests cover a dollar-quote
  payload in it: that it is blocked, that a value containing a `$` sequence still
  parses, and that it survives a round trip through the writer.

  **Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_table/tests/pytests/test_security.py:194-427` @ 031d6f58798d (2026-09-03)

The pairing in those tests is the part worth copying: a rejection test alone
passes just as well against code that rejects everything, so each is paired with
a test that the legitimate value still works.
