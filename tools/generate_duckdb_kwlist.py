#!/usr/bin/env python3
"""
Generate pg_lake_engine/src/pgduck/duckdb_kwlist.h from the vendored DuckDB
keyword lists (src/parser/peg/grammar/keywords/*.list).

The DuckDB keyword set derives from the PostgreSQL keyword list, extended with
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
import subprocess
import sys
import textwrap
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

DUCKDB_SUBMODULE_PATH = "duckdb_pglake/duckdb"
KEYWORDS_RELDIR = "src/parser/peg/grammar/keywords"

SOURCE_DIR = REPO_ROOT / DUCKDB_SUBMODULE_PATH / KEYWORDS_RELDIR

# DuckDB's PEG parser keeps one keyword list per category.  Map each onto the
# PostgreSQL-style category constants defined in keywords.c.  A keyword can
# appear in more than one list (e.g. func_name and type_name); the strictest
# category wins.  bare_label_keyword.list is an orthogonal attribute (whether a
# keyword may be used as a column alias without AS) and is not read.
KEYWORD_LISTS = {
    "unreserved_keyword.list": "UNRESERVED_KEYWORD",
    "column_name_keyword.list": "COL_NAME_KEYWORD",
    "func_name_keyword.list": "TYPE_FUNC_NAME_KEYWORD",
    "type_name_keyword.list": "TYPE_FUNC_NAME_KEYWORD",
    "reserved_keyword.list": "RESERVED_KEYWORD",
}

CATEGORY_RANK = {
    "UNRESERVED_KEYWORD": 0,
    "COL_NAME_KEYWORD": 1,
    "TYPE_FUNC_NAME_KEYWORD": 2,
    "RESERVED_KEYWORD": 3,
}


def submodule_sha() -> str:
    result = subprocess.run(
        ["git", "ls-tree", "HEAD", DUCKDB_SUBMODULE_PATH],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    # Output: "160000 commit <sha>\tduckdb_pglake/duckdb"
    parts = result.stdout.split()
    if len(parts) < 3 or parts[1] != "commit":
        raise RuntimeError(
            f"unexpected git ls-tree output for {DUCKDB_SUBMODULE_PATH}: "
            f"{result.stdout!r}"
        )
    return parts[2]


def read_keyword_lists() -> dict[str, bytes]:
    """Return {list file name: contents} for every file in KEYWORD_LISTS.

    Reads from the submodule when it is checked out, and otherwise fetches
    from github at the SHA the submodule is pinned to, which avoids cloning
    all of DuckDB in CI just to read a few small files.
    """
    if SOURCE_DIR.is_dir():
        return {name: (SOURCE_DIR / name).read_bytes() for name in KEYWORD_LISTS}

    sha = submodule_sha()
    contents = {}
    for name in KEYWORD_LISTS:
        url = (
            f"https://raw.githubusercontent.com/duckdb/duckdb/{sha}/"
            f"{KEYWORDS_RELDIR}/{name}"
        )
        print(f"Fetching {url}", file=sys.stderr)
        with urllib.request.urlopen(url) as resp:
            contents[name] = resp.read()
    return contents


OUTPUT_H = REPO_ROOT / "pg_lake_engine" / "src" / "pgduck" / "duckdb_kwlist.h"


def parse_keywords(lists: dict[str, bytes]) -> list[tuple[str, str]]:
    """Return [(name, category), ...] sorted by name (required for bsearch)."""
    categories: dict[str, str] = {}
    for name, cat in KEYWORD_LISTS.items():
        for line in lists[name].decode("utf-8").splitlines():
            kw = line.strip().lower()
            if not kw:
                continue
            prev = categories.get(kw)
            if prev is None or CATEGORY_RANK[cat] > CATEGORY_RANK[prev]:
                categories[kw] = cat
    return sorted(categories.items())


def sha256_of_lists(lists: dict[str, bytes]) -> str:
    h = hashlib.sha256()
    for name in sorted(lists):
        h.update(name.encode("utf-8") + b"\0" + lists[name])
    return h.hexdigest()


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
         * Source: duckdb_pglake/duckdb/src/parser/peg/grammar/keywords/ (*_keyword.list)
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
        lines.append("{")
        lines.append(f'\t"{name}", {cat}')
        lines.append("},")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the checked-in file is out of date (CI mode).",
    )
    args = parser.parse_args()

    try:
        lists = read_keyword_lists()
    except Exception as e:
        print(
            f"ERROR: submodule {DUCKDB_SUBMODULE_PATH} not checked out "
            f"and remote fetch failed: {e}",
            file=sys.stderr,
        )
        return 1

    source_sha = sha256_of_lists(lists)
    entries = parse_keywords(lists)

    if not entries:
        print("ERROR: no keywords found in the DuckDB keyword lists.", file=sys.stderr)
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
                f"the current {SOURCE_DIR.relative_to(REPO_ROOT)}.\n"
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
