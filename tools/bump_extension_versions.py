#!/usr/bin/env python3
"""
Bump PostgreSQL extension versions across multiple subdirectories.

This script:
  1. Scans subfolders for .control files (marks them as PostgreSQL extensions)
  2. Updates their 'default_version' line to a target version (if different)
  3. Creates a version upgrade SQL file (e.g., pg_lake_engine--2.4--3.0.sql)

Usage:
  python bump_extension_versions.py 3.0
"""

import os
import sys
import re
from pathlib import Path

def find_extensions(base_dir):
    """
    Return a list of top-level directories that contain a .control file.
    (No recursive search — avoids test subfolders, etc.)
    """
    extensions = []
    for item in base_dir.iterdir():
        if not item.is_dir():
            continue
        # Look only directly under this folder for .control files
        control_files = list(item.glob("*.control"))
        if control_files:
            extensions.append(item)
    return extensions

def bump_control_file(control_path, new_version):
    """
    Update the default_version line in the .control file.
    Returns (old_version, changed)
    """
    with open(control_path, "r") as f:
        lines = f.readlines()

    old_version = None
    new_lines = []
    changed = False

    for line in lines:
        if line.strip().startswith("default_version"):
            match = re.search(r"['\"]([^'\"]+)['\"]", line)
            if match:
                old_version = match.group(1)
                if old_version == new_version:
                    # Version is already up to date, no changes needed
                    return old_version, False
                line = re.sub(r"['\"]([^'\"]+)['\"]", f"'{new_version}'", line)
                changed = True
        new_lines.append(line)

    if changed:
        with open(control_path, "w") as f:
            f.writelines(new_lines)

    return old_version, changed

def create_upgrade_sql(ext_dir, ext_name, old_version, new_version):
    """
    Create a stub SQL upgrade file: extname--old--new.sql
    """
    if not old_version:
        print(f"⚠️  No old version found for {ext_name}, skipping SQL file.")
        return
    sql_path = ext_dir / f"{ext_name}--{old_version}--{new_version}.sql"
    if sql_path.exists():
        print(f"✅ SQL upgrade file already exists: {sql_path}")
        return
    with open(sql_path, "w") as f:
        f.write(f"-- Upgrade script for {ext_name} from {old_version} to {new_version}\n")
    print(f"🆕 Created {sql_path}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python bump_extension_versions.py <new_version>")
        sys.exit(1)

    new_version = sys.argv[1]
    repo_root = Path.cwd()

    print(f"🔍 Scanning for PostgreSQL extensions in {repo_root}")
    extensions = find_extensions(repo_root)
    if not extensions:
        print("No extensions found.")
        sys.exit(0)

    for ext_dir in extensions:
        control_files = list(ext_dir.glob("*.control"))
        for control_file in control_files:
            ext_name = control_file.stem
            print(f"\n📦 Updating {ext_name}")
            old_version, changed = bump_control_file(control_file, new_version)

            if not old_version:
                print(f"⚠️ Could not find old version for {ext_name}, skipping SQL file.")
                continue

            if not changed:
                print(f"⏩ {ext_name} already at version {new_version}, skipping.")
                continue

            create_upgrade_sql(ext_dir, ext_name, old_version, new_version)
            print(f"✅ Bumped {ext_name} from {old_version} → {new_version}")

if __name__ == "__main__":
    main()
