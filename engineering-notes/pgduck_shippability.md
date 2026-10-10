# pgduck_server Compatibility Guide for PostgreSQL Integration

This document provides a structured overview of how PostgreSQL features and operations translate when integrated with pgduck_server, particularly focusing on the Foreign Data Wrapper (FDW) aspect in pg_lake_table. The features are classified into three primary categories based on their compatibility and operational behavior in DuckDB.

## Compatibility Categories

- **Non-Shippable Features**: Operations and functions with no current support in DuckDB.
- **Conditionally Shippable Features**: Supported operations that may exhibit differing behaviors or have minor issues.
- **Seamlessly Shippable Features**: Operations and functions with full support and expected identical behavior.

### Non-Shippable Features

These are PostgreSQL features that cannot be directly translated or executed in DuckDB due to the lack of equivalent functionality or data types.

#### Examples

- Lack of support for data types: Certain data types that PostgreSQL support are not supported in DuckDB. To see the full list, check `GetDuckDBTypeForPGType()` function in `pg_lake_engine/src/pgduck/type.c`
- Lack of support for `avg(interval)` in DuckDB, indicating a direct incompatibility for this specific function.
- The absence of the certain data type in DuckDB makes aggregates `min()` and `max()` non-shippable. The same restriction for `min`/`max` aggregates applies for the types for `oid`, `tid`, `pg_lsn`, `money`, `inet`, and `xid8`
- Subscripts on map types (`m[i]`) are not pushed down: in PostgreSQL `m[i]` is the i-th (key, val) pair, while in DuckDB it is the value for key `i`. Key lookups through `map_type.extract()` or `->` are still pushed down.

#### Functions
- regexp_replace is only pushed down when using regexp_replace(text,text,text) or regexp_replace(text,text,text,text), other forms are not available.

### Conditionally Shippable Features

This category includes features that `pgduck_server` supports but might not work identically to PostgreSQL, potentially leading to slight discrepancies or issues.

#### Examples

- **Numeric Averages**: Averages calculated on numeric columns (such as `int`, `double precision`) may yield slightly different results between the two systems.
- **Numeric Sums**: Sums calculated on floating point numbers (such as `double precision`) may yield slightly different results between the two systems. sum(interval) is not available in DuckDB and therefore not pushed down.
- **Float Division**: A division operation like `SELECT 4 / 10;` shows behavioral differences, returning `0` in PostgreSQL but `0.4` in DuckDB.
- `date_trunc` exhibits slightly different behaviour in DuckDB where millennium and century start at 2000 instead of 2001. We correct for that, but only if the field is a constant. In addition, the 3-argument version of date_trunc that specifies a time zone is not available in DuckDB.
- Statistical aggregates `stddev`, `stddev_pop`, `stddev_samp`, `variance`, `var_pop`, `var_samp`  may yield slightly different results between the two systems. Especially  `variance`, `var_pop`, `var_samp` on `real` columns may yield slightly more different results.
- Ordered set aggregates (e.g. percentile_cont, percentile_disc) on interval are not pushed down


### Seamlessly Shippable Features

Features in this category are expected to operate without any compatibility issues, having a one-to-one correspondence in functionality and behavior.

#### Reference

- For detailed lists of supported operations and functions that are considered seamlessly shippable, please refer to:
  - `pg_lake_table/src/fdw/shippable_pgduck_functions.c`
  - `pg_lake_table/src/fdw/shippable_pgduck_operators.c`

### SQL shims

A common approach when trying to push to a function from Postgres to DuckDB that has a different name is to use a SQL shim in the `PG_LAKE_INTERNAL_NSP` (`__lake__internal__nsp__`) schema.  These are usually created in the `pg_lake_engine--XX--XX.sql` migration scripts, and look like the following:

```sql
CREATE FUNCTION __lake__internal__nsp__.regexp_matches(text,text,text)
 RETURNS bool
 LANGUAGE C
 IMMUTABLE PARALLEL SAFE STRICT
AS 'MODULE_PATHNAME', $function$pg_lake_internal_dummy_function$function$;
```

These functions must match the expected name/argument types of the corresponding function on the DuckDB side.  Generally speaking, if you need to replace a postgres function's call with a different function on the DuckDB side you will need to create a `RewriteFuncExpr()` call in [rewrite_query.c](rewrite_query.c) and add the function to the appropriate lookup table, replacing the individual `FuncExpr->funcid` with the placeholder found by `LookupFuncExpr()` in the `PG_LAKE_INTERNAL_NSP`.  (There are plenty of examples in that file.)

#### Troubleshooting

Occasionally you can end up with an error from DuckDB of the form, with `__lake__internal__nsp__` showing up in the deparsed query (example):

```
An error occurred: Catalog Error: Scalar Function with name regexp_matches does not exist!
Did you mean "main.regexp_matches"?
LINE 6:   WHERE __lake__internal__nsp__.regexp_matches(col_text, '.'::text)...
```

There are a couple causes:
 - invalid function id or non-matching arg types
 - more than one function in the `search_path` that matches (`pg_catalog`, `postgis`, and `__lake__internal__nsp__`), so it's ambiguous
 
In the first case, you will just need to find and correct the underlying issue here; presumably in the migration script.

In the second case, you will need to create a different-named trampoline function to disambiguate, then add the appropriate function call in the duckdb_pglake module to call the correct function name.  For instance for `regexp_matches`, we will create a `lake_regexp_matches()` function with the same arguments, and have the DuckDB scalar function just call the `regexp_matches` function for the implementation.
