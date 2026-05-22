# `duckdb-iceberg` local patches

This directory holds local patches that are applied to the
[`duckdb-iceberg`](https://github.com/duckdb/duckdb-iceberg) submodule
(checked out at `../duckdb-iceberg`, pinned to the
[`v1.5-variegata`](https://github.com/duckdb/duckdb-iceberg/tree/v1.5-variegata)
branch) before the iceberg extension is built into `libduckdb.dylib`.

The Makefile's `patch_duckdb` target loops over `duckdb`, `duckdb-postgres`,
and `duckdb-iceberg`, runs `git checkout -f .` + `git clean -fd src/common
src/function src/include` to restore a known-clean state, then applies every
`*.patch` in this directory in lexicographic order.

## `expose_puffin_dv_functions.patch`

### Why this patch exists

`duckdb-iceberg` already implements the Iceberg V3 Deletion Vector codec
internally (see
[`src/core/deletes/iceberg_deletion_vector.cpp`](https://github.com/duckdb/duckdb-iceberg/blob/v1.5-variegata/src/core/deletes/iceberg_deletion_vector.cpp))
and uses it from the iceberg DELETE physical operator. The codec is private
to the C++ class, however - there is no SQL surface that lets another DuckDB
embedding (e.g. `pgduck_server` inside `pg_lake`) consume the Puffin/DV
primitives directly.

Upstream `v1.5-variegata` also intentionally does **not** implement the
full Iceberg Puffin file envelope; `WriteDeletionVectorFile` writes a bare
DV blob to disk and records `content_offset = 0`, relying on the manifest
to point at the blob. That bypasses the Puffin spec (no `PFA1` magic, no
JSON footer) and means files written by duckdb-iceberg cannot currently
be read by Spark or the Iceberg Java reader.

This patch:

1. Adds a small, spec-compliant Puffin envelope reader/writer
   (`IcebergPuffinFile` in `src/common/iceberg_puffin.{hpp,cpp}`) that
   supports `PFA1` magic, multi-blob bodies, and the JSON footer with
   `footer_payload_length`/`flags`/tail magic trailer. The writer always
   emits uncompressed footers (`flags = 0`); the reader rejects
   LZ4-compressed footers explicitly until we add LZ4 frame decode.

   **Spark / Iceberg-Java interoperability.** The footer JSON layout we
   write matches what
   [`apache/iceberg`'s `FileMetadataParser`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/puffin/FileMetadataParser.java)
   reads byte-for-byte:

   - Field order in each blob object mirrors `FileMetadataParser.toJson`
     (`type`, `fields`, `snapshot-id`, `sequence-number`, `offset`,
     `length`, `compression-codec`, `properties`).
   - All six required blob fields are emitted unconditionally. In
     particular `snapshot-id` and `sequence-number` are always written
     - for `deletion-vector-v1` blobs they are `-1` per the spec, which
     is what Spark explicitly expects:
     [puffin-spec.md L96-L98](https://github.com/apache/iceberg/blob/main/format/puffin-spec.md).
     Skipping them (as a naive writer would when the fields default to
     a sentinel) makes Spark's `FileMetadataParser.blobMetadataFromJson`
     raise on `JsonUtil.getLong("snapshot-id")` - i.e. the file is
     unreadable.
   - For `deletion-vector-v1` we also fill the spec-mandated
     `properties.referenced-data-file` + `properties.cardinality` and
     omit `compression-codec` (DV blobs are uncompressed).
   - `Flags` is always written as four zero bytes (no compressed
     footer), which is exactly what
     [`PuffinReader.decodeFlags`](https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/puffin/PuffinReader.java)
     wants: no `FOOTER_PAYLOAD_COMPRESSED` flag bit set.
   - `footer_payload_length` is stored as a little-endian `int32`, which
     matches `PuffinFormat.readIntegerLittleEndian`.

   The `pgduck_server` test
   `test_puffin_footer_is_spark_compatible` hand-decodes the on-disk
   bytes and asserts every one of these invariants, so we catch any
   regression before it ever reaches an actual Spark reader.
2. Refactors `IcebergDeletionVectorData::FromBlob` to call a new
   public static `ParseBlob(...)` helper. This breaks the parser's
   dependency on the iceberg-internal `BoundIcebergManifestEntry` type,
   so the codec can be reused from a SQL function without faking up
   manifest state.
3. Exposes the following public SQL surface (registered by
   `IcebergFunctions::GetTableFunctions` and `::GetScalarFunctions`,
   so a `LOAD iceberg;` is enough to make them visible):

   | Function | Kind | Signature | Purpose |
   | -------- | ---- | --------- | ------- |
   | `iceberg_dv_encode` | scalar | `(BIGINT[]) -> BLOB` | Build a deletion-vector-v1 blob (magic + Roaring bitmaps + CRC) from a list of positions. |
   | `iceberg_dv_decode` | TVF | `(BLOB) -> TABLE(pos BIGINT)` | Round-trip parse a DV blob; CRC mismatch raises. |
   | `iceberg_puffin_read_metadata` | TVF | `(VARCHAR) -> TABLE(blob_idx, blob_type, fields, snapshot_id, sequence_number, blob_offset, blob_length, compression_codec, properties, file_size, footer_compressed)` | Read the Puffin envelope's footer JSON. The `blob_offset`/`blob_length` columns are byte positions within the Puffin file (renamed from the spec names `offset`/`length` to avoid clashing with the DuckDB SQL parser's `OFFSET`/`LENGTH` keywords). |
   | `iceberg_puffin_read_blob` | TVF | `(VARCHAR, BIGINT, BIGINT) -> TABLE(blob BLOB)` | Read a single blob body by absolute offset+length. |
   | `iceberg_puffin_write` | TVF | `(VARCHAR, VARCHAR[], BLOB[]) -> TABLE(blob_idx, blob_type, blob_offset, blob_length, file_size)` | Concatenate the given blobs into a new Puffin file and return their offsets. |
   | `iceberg_dv_positions` | TVF | `(VARCHAR[], BIGINT[], BIGINT[], VARCHAR[]) -> TABLE(file_path, pos)` | Read N DV blobs in one call: `paths[i]@(offsets[i], sizes[i])` is decoded and every deleted position is emitted with `file_path = referenced_data_files[i]`. Mirrors `read_parquet([files…])`: a query that scans N data files issues a SINGLE TVF call instead of an N-leg UNION ALL. The same `path` may appear multiple times (multi-blob Puffin pattern). |
   | `iceberg_write_dvs` | TVF | `(VARCHAR, VARCHAR, BIGINT[]) -> TABLE(referenced_data_file, content_offset, content_size_in_bytes, cardinality, file_size)` | Write a single-DV Puffin file and return the Iceberg V3 manifest fields the caller has to persist. |

The patch is purely additive: it does not modify any existing public
function, the iceberg extension lifecycle, or the DELETE physical
operator. Existing iceberg tests continue to pass unchanged.

### Future integration in `pg_lake_iceberg`

The standalone `iceberg_write_dvs(...)` / `iceberg_dv_positions(...)`
shape is byte-for-byte the SQL the eventual V3 writer/reader in
`pg_lake_iceberg` will issue:

* `pg_lake_table/src/fdw/position_delete_dest.c` (V3 branch) emits
  `iceberg_write_dvs(...)` to materialize a deletion vector when a v3
  table is the delete sink. The result columns map directly onto the
  per-`DataFile` fields the manifest writer in
  `pg_lake_iceberg/src/iceberg/write_table_metadata.c` persists into
  fields 142..145 of the manifest entry record.
* `pg_lake_engine/src/pgduck/read_data.c` (V3 branch) rewrites the
  positional-delete anti-join to point at a single
  `iceberg_dv_positions(paths[], offsets[], sizes[], referenced_data_files[])`
  call instead of the current `read_parquet(<delete files>)`. The four
  parallel arrays are built from the in-memory `DataFile[]` populated
  from the manifest, so a query that scans N data files always issues
  exactly one TVF call (not an N-leg UNION ALL). The TVF returns
  `(file_path, pos)` — identical shape to
  `read_parquet(<positional-delete-files>, filename=true, file_row_number=true)`,
  so the existing anti-join doesn't need to change.

Both call sites have the values they need in C - the offset/size come
out of the in-memory `DataFile` struct populated from the manifest -
so the integration is a SQL-builder change with no new metadata table
on the PG side.

### Re-rebasing when `duckdb-iceberg` bumps

When the submodule pin moves, do:

```sh
cd duckdb_pglake/duckdb-iceberg
git fetch origin
git checkout <new-tag-or-sha>
cd ..
make clean_patches
# attempt to re-apply:
make patch_duckdb || true
```

If the patch hunks no longer apply cleanly (most likely culprits are
`CMakeLists.txt`, `iceberg_functions.cpp`, and the small refactor of
`iceberg_deletion_vector.cpp`), regenerate the patch as follows:

```sh
cd duckdb_pglake/duckdb-iceberg
# fix the conflicts by hand using `git apply --reject` output,
# or hand-merge the file edits.
git diff > ../patches/duckdb-iceberg/expose_puffin_dv_functions.patch
git checkout -f .
git clean -fd src/common src/function src/include
```

The four `+++` new files (`iceberg_puffin.{cpp,hpp}` and
`iceberg_puffin_dv_functions.{cpp,hpp}`) only need to be re-added
verbatim. The interesting hand-merges live in the four modified files.

### Upstreaming

This patch is intended to be filed upstream once stabilized. Tracking
issue / PR will be linked here when it's opened.
