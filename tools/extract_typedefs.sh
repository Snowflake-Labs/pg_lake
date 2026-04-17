#!/bin/bash
#
# Extracts typedef names from C source files in the given directories.
# Handles named structs/enums/unions, anonymous structs/enums/unions,
# function pointers, and simple type aliases.
#
# Usage: tools/extract_typedefs.sh dir1 dir2 ...
# Output: sorted unique type names, one per line (pgindent typedefs format)

set -euo pipefail

if [ $# -eq 0 ]; then
	echo "Usage: $0 dir1 [dir2 ...]" >&2
	exit 1
fi

FILES=$(find "$@" \( -name '*.c' -o -name '*.h' \) \
	-not -path '*/duckdb/*' \
	-not -path '*/avro/*')

{
	# Named structs/enums/unions: typedef struct FooBar
	echo "$FILES" | xargs grep -h '^typedef' | \
		sed -En 's/^typedef[[:space:]]+(struct|enum|union)[[:space:]]+([A-Za-z_][A-Za-z0-9_]*).*/\2/p'

	# Anonymous structs/enums/unions: } FooBar;
	echo "$FILES" | xargs grep -h '^}' | \
		sed -En 's/^\}[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*;.*/\1/p'

	# Function pointers: typedef ... (*FuncName)(...)
	echo "$FILES" | xargs grep -h '^typedef' | \
		sed -En 's/^typedef[[:space:]].*\(\*[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*\).*/\1/p'

	# Simple type aliases: typedef ExistingType NewName;
	# Excludes struct/enum/union and function pointers already handled above
	echo "$FILES" | xargs grep -h '^typedef' | \
		grep -v 'struct\|enum\|union' | grep -v '(\*' | \
		sed -En 's/^typedef[[:space:]]+.+[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*;.*/\1/p'
} | sort -u
