# Postgres Extension Development Skill

You are an expert PostgreSQL extension developer. When helping with Postgres extension development, follow these guidelines and best practices.

## Extension Architecture

### Core Components

1. **Extension Control File** (`.control`)
   - Required metadata file
   - Defines extension name, version, dependencies, schema
   - Example: `pg_lake.control`
   - Prefer relocatable = false and schema = pg_catalog to avoid creating an extension (and any elevated privileges it carries) in a user-defined schema

2. **SQL Script Files**
   - Base script: `extension_name--initial_version.sql`
   - Upgrade scripts: `extension_name--old_version--new_version.sql`
   - Contains DDL for functions, types, tables, etc.
   - New objects always go in the latest upgrade script
   - Upgrade scripts of already-released versions (tags) must not be modified
   - A chain of scripts is executed up to the latest version upon CREATE EXTENSION or ALTER EXTENSION .. UPDATE

3. **C Source Files**
   - Implement extension functionality
   - src/init.c must include `fmgr.h` and use PG_MODULE_MAGIC and define a void _PG_init(void) function
   - Use PGXS build system
   - Postgres extensions can use internal functions and variables declared in postgres headers
   - Many common C functions have wrappers in Postgres

### PGXS Build System

Standard Makefile structure:
```makefile
EXTENSION = extension_name
MODULE_big = $(EXTENSION)
OBJS = $(patsubst %.c,%.o,$(wildcard src/*.c))
DATA = $(wildcard $(EXTENSION)--*--*.sql) $(EXTENSION)--1.0.sql

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
```

Building PostgreSQL from source is helpful for development.

### Key Patterns

#### Function Registration

Define C code:
```c
#include "postgres.h"
#include "fmgr.h"

PG_FUNCTION_INFO_V1(my_function);
Datum
my_function(PG_FUNCTION_ARGS)
{
    // Implementation
    PG_RETURN_DATUM(result);
}
```

and a definition to the SQL script:
```sql
CREATE FUNCTION my_function(argument_name int)
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
```

Functions are by default callable by the public role, should revoke access if the function could expose or modify sensitive data and does not do its own permissions checks.

STRICT functions are noop for NULL input. If the function is not STRICT, then always check for NULL values with `PG_ARGISNULL(n)`

Prefer CREATE over CREATE OR REPLACE for newly introduced functions, to avoid silently replacing user-defined objects.

#### Memory Management
- Use palloc/pfree, NOT malloc/free
- Memory contexts are automatically cleaned up
- Manual cleanup is usually only needed for long loops, or for long-lived contexts (caching)
- CommitTransactionCommand will not restore the old memory context, SPI_finish will

#### Error Handling
- Use `ereport()` for errors
- Add relevant error codes where possible
- Use lowercase of messages, sentences for details and hints
- Use `elog()` for debugging

#### SPI (Server Programming Interface)

```c
SPI_connect();
// Execute queries
int ret = SPI_execute("SELECT ...", read_only, row_limit);
// Process results from SPI_tuptable
SPI_finish();
```
SPI_connect will switch the MemoryContext. Use CurrentMemoryContext from before the call for allocations that need to exist after SPI_finish.

SPI queries will show up in pg_stat_statements, auto_explain, pgaudit. If pg_extension_base is available, use SPI_START() or SPI_EXTENSION_OWNER() and SPI_END().

Queries will run as the current user, use SetUserIdAndSecContext if elevated permissions are needed, but make sure the query cannot be exploited via user-defined operators, functions, views, rules, tables, or triggers. Only interact with extension-owned objects and use qualified names for functions and operators.

## Extension Dependencies

### Declaring Dependencies
In `.control` file:
```
requires = 'extension1, extension2'
```

When using pg_extension_base, dependencies are automatically updated or created during ALTER EXTENSION .. UPDATE.

To call a C function from another extension module, pg_extension_base is required to ensure the other module is loaded first.

extension3 could have the following shared_preload_libraries line to call functions from extension1 and extension2, and additionally preload extension3 itself.
```
#!shared_preload_libraries = '$libdir/extension1,$libdir/extension2,$libdir/extension3'
```

## Testing

### Regression Tests
- Not recommended due to their fragile output

### Python Tests (pytest)
- Useful for integration tests
- Can test with real S3, multiple engines
- Place in `tests/pytests/`

## Debugging with GDB and Core Dumps

### Enabling Core Dumps

Core dumps are essential for debugging crashes in production or test environments.

#### 1. Enable Core Dumps in Shell
```bash
# Set unlimited core dump size
ulimit -c unlimited

# Verify the setting
ulimit -c
```

#### 2. Configure Core Dump Location (Linux)
```bash
# Check current setting
cat /proc/sys/kernel/core_pattern
```

#### 3. Build with Debug Symbols
Ensure your extension is built with debug symbols:
```makefile
# Add to Makefile
CFLAGS += -g -ggdb -O0 -fno-omit-frame-pointer
```

Or rebuild PostgreSQL with debug symbols:
```bash
./configure --enable-debug --enable-cassert CFLAGS="-ggdb -Og -g3 -fno-omit-frame-pointer"
```

### Loading Core Dump in GDB

#### Basic Usage
```bash
# Load core dump with the binary
gdb /path/to/postgres /path/to/core.dump

# Or if core dump is named 'core'
gdb /path/to/postgres core

# For extension shared libraries, gdb will automatically load them
# if they're in the expected location
```

#### Essential GDB Commands

```gdb
# Get backtrace (stack trace)
(gdb) bt

# Get detailed backtrace with all parameters
(gdb) bt full

# Switch to specific frame
(gdb) frame 3

# List source code at current frame
(gdb) list

# Print variable value
(gdb) print variable_name
(gdb) p *pointer_variable

# Print all local variables in current frame
(gdb) info locals

# Print function arguments
(gdb) info args

# Examine memory
(gdb) x/10x address    # 10 hex words
(gdb) x/s string_ptr   # null-terminated string
```

### PostgreSQL-Specific GDB Tips

#### Examining PostgreSQL Structures

```gdb
# Print a Datum as text
(gdb) call DatumGetCString(DirectFunctionCall1(textout, datum_var))

# Print an ErrorData structure
(gdb) print *errordata

# Examine a tuple
(gdb) print *tuple

# Print backend's current query
(gdb) print debug_query_string

# Print memory context information
(gdb) call MemoryContextStats(TopMemoryContext)
```

### Common Debugging Scenarios

#### Segmentation Fault
```gdb
(gdb) bt full
# Look for NULL pointer dereferences
# Check if pointers are valid before dereferencing
# Examine the faulting address in frame 0
```

#### Assertion Failure
```gdb
(gdb) bt
# The backtrace will show the failed assertion
# Examine variables in frames leading up to the assertion
(gdb) frame 2
(gdb) info locals
```

#### Memory Corruption
```gdb
# Print memory context stats to check for corruption
(gdb) call MemoryContextCheck(TopMemoryContext)

# Examine suspicious pointers
(gdb) p *suspicious_pointer
(gdb) x/20x suspicious_pointer
```

### Generating Core Dumps on Demand

For hung processes:
```bash
# Send ABRT signal to generate core dump
kill -ABRT <postgres_pid>

# Or use gcore to generate without killing process
gcore <postgres_pid>
```

### Analyzing Core Dumps Without GDB

```bash
# Quick stack trace
gdb -batch -ex "bt" /path/to/postgres /path/to/core.dump

# Full backtrace
gdb -batch -ex "bt full" /path/to/postgres /path/to/core.dump
```

## Useful PostgreSQL Headers

- `postgres.h` - Must be first
- `fmgr.h` - Function manager
- `catalog/pg_*.h` - System catalog definitions
- `utils/builtins.h` - Built-in functions
- `utils/array.h` - Array handling
- `executor/spi.h` - SPI interface
- `access/htup_details.h` - Tuple handling

## Building and Installing

```bash
# Build
make

# Install
make install

# Run tests
make check          # Unit tests
make installcheck   # Integration tests with installed extension

# Clean
make clean
```
