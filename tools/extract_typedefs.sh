#!/bin/bash
#
# Extracts typedef names from C source files in the given directories.
# Output: sorted unique type names, one per line (pgindent typedefs format)
#
# Usage: tools/extract_typedefs.sh dir1 dir2 ...

set -euo pipefail

if [ $# -eq 0 ]; then
	echo "Usage: $0 dir1 [dir2 ...]" >&2
	exit 1
fi

# Single grep pass: collect all typedef and closing-brace lines
LINES=$(find "$@" \( -name '*.c' -o -name '*.h' \) \
	-not -path '*/duckdb/*' \
	-not -path '*/avro/*' \
	-print0 | \
	xargs -0 grep -hE '^(typedef|})')

{
	# typedef struct/enum/union Name ...
	echo "$LINES" | sed -En 's/^typedef[[:space:]]+(struct|enum|union)[[:space:]]+([A-Za-z_][A-Za-z0-9_]*).*/\2/p'

	# } Name; (anonymous struct/enum/union)
	echo "$LINES" | sed -En 's/^\}[[:space:]]+([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*;.*/\1/p'

	# typedef ... (*Name)(...) (function pointers)
	echo "$LINES" | sed -En 's/^typedef.*\(\*[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*\).*/\1/p'

	# typedef ... Name; (simple aliases, also catches typedef struct X Y;)
	echo "$LINES" | sed -En 's/^typedef.*[[:space:]]([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*;[[:space:]]*$/\1/p'
} | sort -u
