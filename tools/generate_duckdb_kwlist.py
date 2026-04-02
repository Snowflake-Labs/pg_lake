#!/usr/bin/env python3
"""
Generate pg_lake_engine/src/pgduck/duckdb_kwlist.h from the vendored DuckDB
keyword list.

The DuckDB parser uses a fork of the PostgreSQL keyword list, extended with
DuckDB-specific reserved words (PIVOT, LAMBDA, QUALIFY, etc.) that PostgreSQL
does not know about.  Without this list, quote_identifier() misses those words
and pushdown queries referencing columns with those names silently fail.

Usage:
  # Regenerate the header (checked-in artefact):
  python3 tools/generate_duckdb_kwlist.py

  # CI / staleness check — exits non-zero if the checked-in file is out of date:
  python3 tools/generate_duckdb_kwlist.py --check

The generated file is committed to the repository.  Re-run this script
whenever duckdb_pglake/duckdb is updated to a new DuckDB release.
"""

import argparse
import hashlib
import re
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

SOURCE_HPP = (
    REPO_ROOT
    / "duckdb_pglake"
    / "duckdb"
    / "third_party"
    / "libpg_query"
    / "include"
    / "parser"
    / "kwlist.hpp"
)

OUTPUT_H = REPO_ROOT / "pg_lake_engine" / "src" / "pgduck" / "duckdb_kwlist.h"

# Map category strings found in the .hpp to the integer constants defined in
# keywords.c.  These must match the #defines there.
CATEGORY_MAP = {
    "UNRESERVED_KEYWORD": "UNRESERVED_KEYWORD",
    "COL_NAME_KEYWORD": "COL_NAME_KEYWORD",
    "TYPE_FUNC_NAME_KEYWORD": "TYPE_FUNC_NAME_KEYWORD",
    "RESERVED_KEYWORD": "RESERVED_KEYWORD",
}

KW_RE = re.compile(r'PG_KEYWORD\(\s*"([^"]+)"\s*,\s*\w+\s*,\s*(\w+)\s*\)')


def parse_keywords(hpp_text: str) -> list[tuple[str, str]]:
    """Return [(name, category), ...] sorted by name (required for bsearch)."""
    entries = []
    for m in KW_RE.finditer(hpp_text):
        name, cat = m.group(1), m.group(2)
        if cat not in CATEGORY_MAP:
            print(
                f"WARNING: unknown category {cat!r} for keyword {name!r}",
                file=sys.stderr,
            )
            continue
        entries.append((name, CATEGORY_MAP[cat]))
    entries.sort(key=lambda e: e[0])
    return entries


def sha256_of_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def render(entries: list[tuple[str, str]], source_sha256: str) -> str:
    lines = []
    lines.append(
        textwrap.dedent(
            f"""\
        /*
         * duckdb_kwlist.h
         *
         * AUTO-GENERATED — do not edit by hand.
         * Run:  python3 tools/generate_duckdb_kwlist.py
         *
         * Source: duckdb_pglake/duckdb/third_party/libpg_query/include/parser/kwlist.hpp
         * Source SHA-256: {source_sha256}
         *
         * This file is included by pg_lake_engine/src/pgduck/keywords.c to
         * initialise the DuckDB keyword lookup table used by
         * duckdb_quote_identifier().
         *
         * The list must remain sorted alphabetically for bsearch() to work.
         */
        """
        )
    )
    for name, cat in entries:
        lines.append(f'\t{{"{name}", {cat}}},')
    lines.append("")  # trailing newline
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the checked-in file is out of date (CI mode).",
    )
    args = parser.parse_args()

    if not SOURCE_HPP.exists():
        print(f"ERROR: source file not found: {SOURCE_HPP}", file=sys.stderr)
        print("Has the duckdb submodule been initialised?", file=sys.stderr)
        return 1

    hpp_text = SOURCE_HPP.read_text(encoding="utf-8")
    source_sha = sha256_of_file(SOURCE_HPP)
    entries = parse_keywords(hpp_text)

    if not entries:
        print("ERROR: no PG_KEYWORD entries found — check the regex.", file=sys.stderr)
        return 1

    generated = render(entries, source_sha)

    if args.check:
        if not OUTPUT_H.exists():
            print(
                f"ERROR: {OUTPUT_H} does not exist — run generate_duckdb_kwlist.py",
                file=sys.stderr,
            )
            return 1
        existing = OUTPUT_H.read_text(encoding="utf-8")
        if existing == generated:
            print(f"OK: {OUTPUT_H.relative_to(REPO_ROOT)} is up to date.")
            return 0
        else:
            print(
                f"STALE: {OUTPUT_H.relative_to(REPO_ROOT)} does not match "
                f"the current {SOURCE_HPP.relative_to(REPO_ROOT)}.\n"
                f"Run:  python3 tools/generate_duckdb_kwlist.py",
                file=sys.stderr,
            )
            return 1

    OUTPUT_H.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_H.write_text(generated, encoding="utf-8")
    print(f"Wrote {OUTPUT_H.relative_to(REPO_ROOT)} ({len(entries)} keywords)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
