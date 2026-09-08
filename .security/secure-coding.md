# Secure coding practices

> Derived-from: Snowflake-Labs/pg_lake@031d6f58798d · generated 2026-09-03
> Regenerate when: a `test_security.py` suite is added or removed; a suite stops running in CI, or the `pg_version` matrices change; the `check-indent` or `check-duckdb-kwlist` gate is removed; or a new URL gate ships without a test in one of these suites.

The controls in `trust-boundaries.md` are single `if` statements in a large tree,
so what keeps them alive is the test suites that fail when one is removed. This
document says which those are, what CI runs them on, and where the coverage runs
out.

## The security regression suites

Four suites, one per extension that has a boundary to defend. Each one opens by
naming the vulnerability class it exists for.

| Suite | Tests | Class |
| --- | --- | --- |
| `pg_lake_copy/tests/pytests/test_security.py` | 21 | privilege-check bypass and SSRF |
| `pg_lake_iceberg/tests/pytests/test_security.py` | 15 | Avro parsing, nested metadata paths, credential redaction |
| `pg_lake_table/tests/pytests/test_security.py` | 5 | privilege escalation and SQL injection |
| `pg_lake_benchmark/tests/pytests/test_security.py` | 4 | SQL injection through a `location` argument |

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_copy/tests/pytests/test_security.py:1-4`, `pg_lake_iceberg/tests/pytests/test_security.py:1-3`, `pg_lake_table/tests/pytests/test_security.py:1-4`, `pg_lake_benchmark/tests/pytests/test_security.py:1-4` @ 031d6f58798d (2026-09-03)

Three properties of how they are written are worth keeping.

**They test through the product surface, as an unprivileged user.** The `COPY`
tests connect as a role with no lake privileges and try to read `/etc/passwd`,
`/tmp/test.json` and `/home/postgres/.pgpass`. They assert on the behaviour, not
on the presence of a check in the source.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_copy/tests/pytests/test_security.py:38-130` @ 031d6f58798d (2026-09-03)

**Every rejection is paired with an acceptance.** `test_clean_s3_url_still_works`
sits next to the query-string rejections, `test_s3_url_with_safe_query_param_is_accepted`
next to the allowlist, `test_azure_container_url_still_works` next to the Azure
host rejections. A rejection test on its own would keep passing against code that
rejects everything, which is a real way for a gate to rot into a denial of
service.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_copy/tests/pytests/test_security.py:265-288,338-360,518-561` @ 031d6f58798d (2026-09-03)

**The nested cases are covered, not just the top-level one.** For Iceberg the
attacker-controlled value can arrive inside a manifest list, a manifest, or a
data-file entry, and each of those is a separate test. One of them,
`test_manifest_url_check_does_not_require_lake_read`, pins the deliberate
asymmetry that the scheme check on a metadata-derived path is not also a role
check.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_iceberg/tests/pytests/test_security.py:272-429` @ 031d6f58798d (2026-09-03)

The escalation tests in `pg_lake_table` go further and assert on the outcome
rather than the error: they check `rolsuper` on the test user afterwards, so the
test fails if the payload succeeds even when the statement also raises.

**Basis:** code-verified Snowflake-Labs/pg_lake `pg_lake_table/tests/pytests/test_security.py:142-152` @ 031d6f58798d (2026-09-03)

**Bounded by:** these suites cover the gates that exist. They cannot fail for a
new entry point that never got a gate, because nothing enumerates the entry
points. The call-site list in `trust-boundaries.md` is the closest thing to that
enumeration, and it is a snapshot, which is why the `Regenerate when:` line on
that document leads with a new call site appearing.

## What CI runs

Every suite runs on PostgreSQL 17 and 18 on AlmaLinux, as separate make targets.
Builds cover 16, 17 and 18.

**Basis:** code-verified Snowflake-Labs/pg_lake `.github/workflows/pytest_all.yml:93-94,118-119,149-163` @ 031d6f58798d (2026-09-03)

`check-pg_lake_table` is split across five parallel workers because it is slow;
the other targets run whole. The SQL regression suites run separately, on a
Debian image, so both distributions are exercised.

**Basis:** code-verified Snowflake-Labs/pg_lake `.github/workflows/pytest_all.yml:188-194,206-207,209-243` @ 031d6f58798d (2026-09-03)

**Bounded by:** the pytest matrices are 17 and 18 only, so none of the four
security suites ever runs on PostgreSQL 16 even though 16 is built and its SQL
regression suites do run. A version-specific behaviour difference in a gate on 16
would not be caught. `install.sh` is not exercised by CI at all either, which
matters because it is the script that writes `shared_preload_libraries` and
prints the pgduck_server invocation (`deployment.md`).

## The two static gates

**Formatting.** `make check-indent` runs `pgindent` against a typedefs list
downloaded for a pinned PostgreSQL version, and CI runs the same target, so local
and CI formatting agree.

**Basis:** code-verified Snowflake-Labs/pg_lake `Makefile:52-55,70-85`, `.github/workflows/lint.yml:53-55` @ 031d6f58798d (2026-09-03)

**The DuckDB keyword table.** `make check-duckdb-kwlist` fails when the
committed keyword table no longer matches the vendored DuckDB's `kwlist.hpp`.
This is the only gate in the repository that catches a *dependency bump*
invalidating a security-relevant table: a new DuckDB reserved word that is not in
the committed list means `duckdb_quote_identifier` under-quotes it
(`input-rendering.md`).

**Basis:** code-verified Snowflake-Labs/pg_lake `Makefile:229-238`, `.github/workflows/lint.yml:57-59`, `tools/generate_duckdb_kwlist.py:1-21` @ 031d6f58798d (2026-09-03)

**Bounded by:** the generator can fetch `kwlist.hpp` over the network from GitHub
at the pinned submodule SHA when the submodule is not checked out, so in that mode
the check trusts the fetch. There is no linting for the pattern that caused the
escalation in `input-rendering.md` either: nothing flags a new `$$`-wrapped
generated statement, or a new call site that skips a URL gate.

**Basis:** code-verified Snowflake-Labs/pg_lake `tools/generate_duckdb_kwlist.py:28,39-41` @ 031d6f58798d (2026-09-03)

## Conventions worth following

Drawn from the code that already exists, so a change that departs from them is
worth a second look:

- **A gate goes at the entry point, and every entry point gets one.** Adding a
  function that takes a path or a URL means adding a `CheckURLReadAccess` or
  `CheckURLWriteAccess` call, and the FDW pattern of calling the gate with `NULL`
  first for the role check and again with the value once it is known is the
  worked example.
- **Allowlist, never denylist.** All four URL controls are allowlists, and the
  comment on the query-parameter one gives the reason: the set of settings DuckDB
  honours grows, so a denylist rots into a bypass.
- **State the bypass in the comment.** The comments on the local-path check, the
  http(s) write refusal and the Azure container-part check each name the concrete
  attack. That is what let this bundle cite an intent rather than guess one.
- **Generated SQL is a bound parameter, not an interpolation.** Use
  `DECLARE_SPI_ARGS` / `SPI_ARG_VALUE`; do not wrap a generated statement in
  `$$...$$`.
- **A new gate ships with a paired test.** One that the payload is rejected, one
  that the legitimate value still works.
- **Credentials go on user mappings.** A GUC fallback is a cluster-wide identity;
  see `data-and-secrets.md`.
- **A privilege-relevant GUC is `PGC_SUSET` at least,** and
  `GUC_SUPERUSER_ONLY` if it carries a credential.

## Reporting

Security issues in pg_lake should be reported privately to the maintainers rather
than in a public issue. At this commit the repository has no `SECURITY.md`, so
there is no documented reporting address in-tree; `.github/CODEOWNERS` names the
maintaining team.

**Basis:** code-verified Snowflake-Labs/pg_lake `.github/CODEOWNERS:8-14` (no `SECURITY.md` in the repository root or in `.github/`) @ 031d6f58798d (2026-09-03)
