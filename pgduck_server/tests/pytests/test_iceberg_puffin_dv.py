"""
pgduck-level (no PG involvement) sanity tests for the Iceberg Puffin + DV
primitives exposed by the patch in
``duckdb_pglake/patches/duckdb-iceberg/expose_puffin_dv_functions.patch``.

These tests run pure SQL against the freshly-built libduckdb that pgduck_server
embeds. They are deliberately minimal in moving parts: no manifest, no
pg_lake_iceberg, no PG catalog. Every scenario writes a Puffin file (or
DV blob) to S3 via ``iceberg_puffin_write`` / ``iceberg_dv_encode`` /
``iceberg_write_dvs`` and reads it back via ``iceberg_puffin_read_metadata``
/ ``iceberg_puffin_read_blob`` / ``iceberg_dv_decode`` /
``iceberg_dv_positions``.
"""

import pytest
from utils_pytest import *


@pytest.fixture(scope="module", autouse=True)
def _load_iceberg(pgduck_conn):
    """``autoload_known_extensions=true`` should kick in on first call, but
    forcing a LOAD up front gives a clean failure mode if the iceberg
    extension was not built into libduckdb for some reason."""
    run_command("LOAD iceberg;", pgduck_conn)


def _puffin_path(s3, name):
    return f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv/{name}"


def test_puffin_roundtrip_single_blob(pgduck_conn, s3):
    """Write one Puffin blob of known bytes and assert that
    ``iceberg_puffin_read_metadata`` + ``iceberg_puffin_read_blob`` returns
    the same bytes at the offset advertised by the footer."""
    path = _puffin_path(s3, "single_blob.puffin")
    payload_hex = "0102030405060708090a0b0c0d0e0f10"

    written = run_query(
        f"""
        SELECT blob_idx, blob_type, blob_offset, blob_length, file_size
        FROM iceberg_puffin_write(
            '{path}',
            ['my-type'::VARCHAR],
            [from_hex('{payload_hex}')::BLOB]
        )
        """,
        pgduck_conn,
    )
    assert len(written) == 1
    blob_idx, blob_type, offset, length, file_size = written[0]
    assert int(blob_idx) == 0
    assert blob_type == "my-type"
    # head magic (4 bytes) is at offset 0; the first blob payload starts right after.
    assert int(offset) == 4
    assert int(length) == 16
    assert int(file_size) > int(offset) + int(length)

    md = run_query(
        f"""
        SELECT blob_idx, blob_type, blob_offset, blob_length, file_size
        FROM iceberg_puffin_read_metadata('{path}')
        """,
        pgduck_conn,
    )
    assert md == written  # spec invariant: writer output matches the footer.

    blob = run_query(
        f"SELECT lower(hex(blob)) FROM iceberg_puffin_read_blob('{path}', {offset}, {length})",
        pgduck_conn,
    )
    assert blob[0][0] == payload_hex


def test_puffin_roundtrip_multi_blob(pgduck_conn, s3):
    """Five blobs with distinct types must keep their order and bytes."""
    path = _puffin_path(s3, "multi_blob.puffin")
    types = [f"type-{i}" for i in range(5)]
    payloads_hex = ["aa" * (i + 1) for i in range(5)]  # 1..5 bytes each
    types_sql = ", ".join(f"'{t}'" for t in types)
    payloads_sql = ", ".join(f"from_hex('{p}')::BLOB" for p in payloads_hex)

    written = run_query(
        f"""
        SELECT blob_idx, blob_type, blob_offset, blob_length
        FROM iceberg_puffin_write('{path}', [{types_sql}]::VARCHAR[], [{payloads_sql}]::BLOB[])
        ORDER BY blob_idx
        """,
        pgduck_conn,
    )
    assert len(written) == 5
    for i, row in enumerate(written):
        assert int(row[0]) == i
        assert row[1] == f"type-{i}"
        assert int(row[3]) == (i + 1)
    offsets = [int(r[2]) for r in written]
    assert offsets[0] == 4
    for i in range(1, 5):
        assert offsets[i] == offsets[i - 1] + i

    for i, row in enumerate(written):
        bytes_back = run_query(
            f"SELECT lower(hex(blob)) FROM iceberg_puffin_read_blob('{path}', {row[2]}, {row[3]})",
            pgduck_conn,
        )
        assert bytes_back[0][0] == payloads_hex[i]


def test_puffin_magic_bytes(pgduck_conn, s3):
    """First 4 bytes of any Puffin file must be the magic ``PFA1``."""
    path = _puffin_path(s3, "magic.puffin")
    run_command(
        f"""
        SELECT * FROM iceberg_puffin_write(
            '{path}', ['t']::VARCHAR[], [from_hex('00')::BLOB]
        )
        """,
        pgduck_conn,
    )
    magic = run_query(
        f"SELECT lower(hex(blob)) FROM iceberg_puffin_read_blob('{path}', 0, 4)",
        pgduck_conn,
    )
    assert magic[0][0] == "50464131"  # 'P','F','A','1' in hex


@pytest.mark.parametrize(
    "label,positions",
    [
        ("empty", []),
        ("single", [42]),
        ("dense_low32", list(range(0, 256))),
        ("sparse_low32", [0, 1_000, 1_000_000, 4_294_000_000]),
        ("high32_only", [1 << 32, (1 << 32) + 1, (1 << 33) + 5]),
        ("mixed_high_low", [0, 1, (1 << 32), (1 << 32) + 1, (1 << 33), (1 << 33) + 7]),
    ],
)
def test_dv_encode_decode_roundtrip(pgduck_conn, s3, label, positions):
    """Encode -> decode must reconstruct the exact set of positions."""
    arr_sql = "ARRAY[" + ",".join(f"{p}::BIGINT" for p in positions) + "]"
    # iceberg_dv_encode of an empty list still produces a valid (empty) DV
    # blob; iceberg_dv_decode then returns zero rows. We cast on the empty
    # branch because DuckDB cannot infer the element type from ARRAY[].
    encode_sql = (
        f"iceberg_dv_encode(CAST({arr_sql} AS BIGINT[]))"
        if not positions
        else f"iceberg_dv_encode({arr_sql})"
    )
    decoded = run_query(
        f"SELECT pos FROM iceberg_dv_decode({encode_sql}) ORDER BY pos",
        pgduck_conn,
    )
    assert [int(r[0]) for r in decoded] == sorted(set(positions)), label


def test_dv_blob_framing(pgduck_conn, s3):
    """Inspect the DV blob bytes: leading uint32 length (big-endian) then magic
    bytes D1 D3 39 64 — exactly the deletion-vector-v1 framing from the spec."""
    row = run_query(
        """
        SELECT lower(hex(iceberg_dv_encode(ARRAY[1::BIGINT, 2, 3]))) AS h,
               octet_length(iceberg_dv_encode(ARRAY[1::BIGINT, 2, 3])) AS l
        """,
        pgduck_conn,
    )
    hex_blob = row[0][0]
    blob_len = int(row[0][1])
    # leading uint32 length (BE), then the magic D1 D3 39 64
    assert hex_blob[8:16].lower() == "d1d33964"
    # blob_len includes 4 bytes leading length + 4 magic + 8 bitmap-count + bitmap bytes + 4 CRC
    assert blob_len >= 20


def test_dv_decode_rejects_bad_magic(pgduck_conn, s3):
    """A DV blob that does not look like a deletion-vector-v1 (we hand it a
    too-short payload) must be rejected with a clear error rather than a
    crash."""
    # 8 bytes is below the minimum for a deletion-vector-v1 (which needs at
    # least 4-byte leading vector-size + 4-byte magic + 4-byte CRC). The
    # implementation should raise before reading past the buffer.
    with pytest.raises(Exception) as exc:
        run_query(
            "SELECT * FROM iceberg_dv_decode(from_hex('0000000400000000'))",
            pgduck_conn,
        )
    pgduck_conn.rollback()
    msg = str(exc.value).lower()
    assert any(
        tok in msg
        for tok in ("magic", "corrupt", "checksum", "too small", "deletion-vector")
    )


def test_dv_decode_rejects_bad_crc(pgduck_conn, s3):
    """Corrupt the LAST byte of a valid DV blob (always part of the CRC)
    and assert the decode raises with a checksum error."""
    # Build a real DV via the scalar, drop the trailing CRC byte and append
    # 0xff to force a CRC mismatch. We pre-materialize the DV blob in Python
    # so the table function does not need a subquery argument.
    dv_row = run_query(
        "SELECT lower(hex(iceberg_dv_encode(ARRAY[1::BIGINT, 2, 3])))",
        pgduck_conn,
    )
    dv_hex = dv_row[0][0]
    tampered_hex = dv_hex[:-2] + "ff"
    with pytest.raises(Exception) as exc:
        run_query(
            f"SELECT * FROM iceberg_dv_decode(from_hex('{tampered_hex}'))",
            pgduck_conn,
        )
    pgduck_conn.rollback()
    assert any(
        tok in str(exc.value).lower()
        for tok in ("checksum", "corrupt", "computed checksum")
    )


def test_iceberg_write_dvs_simple(pgduck_conn, s3):
    """``iceberg_write_dvs`` returns the manifest fields the future pg_lake
    integration will persist, and the produced file round-trips through
    ``iceberg_dv_positions``."""
    path = _puffin_path(s3, "single_dv.puffin")
    data_file = f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv/data-A.parquet"
    positions = [2, 5, 50, 99]

    write_row = run_query(
        f"""
        SELECT referenced_data_file, content_offset, content_size_in_bytes, cardinality, file_size
        FROM iceberg_write_dvs(
            '{path}',
            '{data_file}',
            ARRAY{positions}::BIGINT[]
        )
        """,
        pgduck_conn,
    )
    assert len(write_row) == 1
    ref, content_offset, content_size, cardinality, file_size = write_row[0]
    assert ref == data_file
    assert int(cardinality) == len(positions)
    assert int(content_offset) == 4  # head magic only precedes the blob.
    assert int(content_size) > 0
    assert int(file_size) > int(content_offset) + int(content_size)

    # The Puffin footer's offset/length for this blob MUST equal the values
    # the writer just handed us. This is the spec invariant from the
    # "Data File Fields" note about content_offset/content_size_in_bytes
    # matching the Puffin footer's offset/length.
    footer_row = run_query(
        f"""
        SELECT blob_offset, blob_length, blob_type
        FROM iceberg_puffin_read_metadata('{path}')
        WHERE blob_idx = 0
        """,
        pgduck_conn,
    )
    assert int(footer_row[0][0]) == int(content_offset)
    assert int(footer_row[0][1]) == int(content_size)
    assert footer_row[0][2] == "deletion-vector-v1"

    decoded = run_query(
        f"""
        SELECT file_path, pos
        FROM iceberg_dv_positions(
            ['{path}'], [{content_offset}::BIGINT], [{content_size}::BIGINT], ['{data_file}']
        )
        ORDER BY pos
        """,
        pgduck_conn,
    )
    # The TVF stamps every output row's file_path with the matching entry from
    # the referenced_data_files array (here a single-element array).
    assert all(r[0] == data_file for r in decoded)
    assert [int(r[1]) for r in decoded] == positions


def test_iceberg_dv_positions_multi_data_files(pgduck_conn, s3):
    """The whole point of the array-form signature: a query that scans N
    data files (each with its own DV) issues a SINGLE iceberg_dv_positions
    call instead of an N-leg UNION ALL. We exercise N=4 across 2 distinct
    Puffin files (data files A,B share a multi-blob Puffin; C,D each have
    their own single-blob Puffin) and verify the per-blob (file_path, pos)
    pairs come out correctly."""
    # Build four DVs across THREE Puffin files. The first two share one
    # Puffin file (multi-blob Puffin pattern) so we also exercise that
    # the same `path` element can appear multiple times.
    multi_puffin = _puffin_path(s3, "multi/shared.puffin")
    single_b = _puffin_path(s3, "multi/b.puffin")
    single_c = _puffin_path(s3, "multi/c.puffin")
    df_a = f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv/multi-A.parquet"
    df_b = f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv/multi-B.parquet"
    df_c = f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv/multi-C.parquet"
    df_d = f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv/multi-D.parquet"

    # iceberg_write_dvs writes a single-blob Puffin file, so we hand-roll the
    # multi-blob one via iceberg_puffin_write + iceberg_dv_encode.
    run_command(
        f"""
        SELECT * FROM iceberg_puffin_write(
            '{multi_puffin}',
            ['deletion-vector-v1', 'deletion-vector-v1']::VARCHAR[],
            [iceberg_dv_encode(ARRAY[1::BIGINT, 3, 5]),
             iceberg_dv_encode(ARRAY[7::BIGINT, 9])]::BLOB[]
        )
        """,
        pgduck_conn,
    )
    multi_meta = run_query(
        f"""
        SELECT blob_idx, blob_offset, blob_length
        FROM iceberg_puffin_read_metadata('{multi_puffin}') ORDER BY blob_idx
        """,
        pgduck_conn,
    )
    a_off, a_len = int(multi_meta[0][1]), int(multi_meta[0][2])
    b_off, b_len = int(multi_meta[1][1]), int(multi_meta[1][2])

    # Two single-blob puffins via iceberg_write_dvs, one per data file.
    c_row = run_query(
        f"SELECT content_offset, content_size_in_bytes FROM iceberg_write_dvs("
        f"'{single_c}', '{df_c}', ARRAY[2::BIGINT, 4])",
        pgduck_conn,
    )
    d_row = run_query(
        f"SELECT content_offset, content_size_in_bytes FROM iceberg_write_dvs("
        f"'{single_b}', '{df_d}', ARRAY[8::BIGINT, 10, 12])",
        pgduck_conn,
    )
    c_off, c_len = int(c_row[0][0]), int(c_row[0][1])
    d_off, d_len = int(d_row[0][0]), int(d_row[0][1])

    rows = run_query(
        f"""
        SELECT file_path, pos
        FROM iceberg_dv_positions(
            ['{multi_puffin}', '{multi_puffin}', '{single_c}', '{single_b}'],
            [{a_off}::BIGINT, {b_off}::BIGINT, {c_off}::BIGINT, {d_off}::BIGINT],
            [{a_len}::BIGINT, {b_len}::BIGINT, {c_len}::BIGINT, {d_len}::BIGINT],
            ['{df_a}', '{df_b}', '{df_c}', '{df_d}']
        )
        ORDER BY file_path, pos
        """,
        pgduck_conn,
    )
    grouped = {}
    for fp, p in rows:
        grouped.setdefault(fp, []).append(int(p))
    assert grouped[df_a] == [1, 3, 5]
    assert grouped[df_b] == [7, 9]
    assert grouped[df_c] == [2, 4]
    assert grouped[df_d] == [8, 10, 12]


def test_iceberg_dv_positions_array_length_mismatch(pgduck_conn, s3):
    """The TVF must reject mismatched array lengths up front rather than
    silently truncate or read past the end of one array."""
    path = _puffin_path(s3, "mismatch/a.puffin")
    df = f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv/mismatch-A.parquet"
    run_command(
        f"SELECT * FROM iceberg_write_dvs('{path}', '{df}', ARRAY[1::BIGINT, 2])",
        pgduck_conn,
    )
    with pytest.raises(Exception) as exc:
        run_query(
            f"""
            SELECT * FROM iceberg_dv_positions(
                ['{path}', '{path}'],   -- 2 paths
                [4::BIGINT],            -- 1 offset (mismatch)
                [8::BIGINT, 8],
                ['{df}', '{df}']
            )
            """,
            pgduck_conn,
        )
    pgduck_conn.rollback()
    assert (
        "equal length" in str(exc.value).lower() or "length" in str(exc.value).lower()
    )


def test_iceberg_dv_positions_empty_arrays(pgduck_conn, s3):
    """Calling with all-empty arrays yields zero rows - this is the 'no
    DVs at all in the scan' shape that the SQL builder will emit when
    the manifest has no V3 delete files."""
    rows = run_query(
        """
        SELECT file_path, pos
        FROM iceberg_dv_positions(
            []::VARCHAR[], []::BIGINT[], []::BIGINT[], []::VARCHAR[]
        )
        """,
        pgduck_conn,
    )
    assert rows == []


def test_puffin_read_metadata_rejects_non_puffin(pgduck_conn, s3):
    """A file without the PFA1 magic must fail metadata read with a clear
    error message, not crash."""
    path = _puffin_path(s3, "not_a_puffin.csv")
    run_command(
        f"COPY (SELECT 1 AS id, 'hello' AS val FROM generate_series(1,20)) TO '{path}'",
        pgduck_conn,
    )
    with pytest.raises(Exception) as exc:
        run_query(f"SELECT * FROM iceberg_puffin_read_metadata('{path}')", pgduck_conn)
    pgduck_conn.rollback()
    assert "puffin" in str(exc.value).lower() or "magic" in str(exc.value).lower()


def test_puffin_footer_is_spark_compatible(pgduck_conn, s3):
    """Hand-roll the exact validation that Iceberg's Java
    ``FileMetadataParser.blobMetadataFromJson`` does (see
    apache/iceberg core/src/main/java/org/apache/iceberg/puffin/
    FileMetadataParser.java). All of ``type``, ``fields``,
    ``snapshot-id``, ``sequence-number``, ``offset``, ``length`` are read
    with ``JsonUtil.getString``/``getLong``/``getIntegerList`` which raise
    on a missing field, so a writer that skips any of those produces a
    file that Spark and the Iceberg Java reader CANNOT open.

    This test pulls the raw footer JSON straight out of the bytes we
    wrote and asserts every required key is present and well-typed,
    plus the deletion-vector-v1 specific contract:
    snapshot-id == sequence-number == -1 and the ``referenced-data-file``
    + ``cardinality`` properties are populated."""
    import json
    import struct

    path = _puffin_path(s3, "spark_compat.puffin")
    data_file = f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv/data-spark.parquet"
    positions = [0, 1, 2, 5, 99]
    run_command(
        f"""
        SELECT * FROM iceberg_write_dvs(
            '{path}', '{data_file}', ARRAY{positions}::BIGINT[]
        )
        """,
        pgduck_conn,
    )

    # Pull the entire file as one blob via read_blob so we can verify the
    # on-disk byte layout (magics + footer trailer + JSON payload) without
    # having to mount an S3 client in the test.
    size_row = run_query(
        f"SELECT file_size FROM iceberg_puffin_read_metadata('{path}')",
        pgduck_conn,
    )
    file_size = int(size_row[0][0])
    raw_hex = run_query(
        f"SELECT lower(hex(blob)) FROM iceberg_puffin_read_blob('{path}', 0, {file_size})",
        pgduck_conn,
    )[0][0]
    raw = bytes.fromhex(raw_hex)

    assert raw[:4] == b"PFA1", "head magic must be PFA1"
    assert raw[-4:] == b"PFA1", "tail magic must be PFA1"

    # Footer trailer (last 12 bytes): footer_payload_length (i32 LE),
    # flags (4 bytes), tail magic (4 bytes).
    footer_payload_length = struct.unpack("<i", raw[-12:-8])[0]
    flags = raw[-8:-4]
    assert (
        flags == b"\x00\x00\x00\x00"
    ), f"writer must always emit Flags=0 (uncompressed); got {flags.hex()}"
    assert footer_payload_length > 0

    # Footer payload starts 4 bytes after the footer-start magic and is
    # exactly footer_payload_length bytes long.
    footer_start_magic_offset = file_size - 12 - footer_payload_length - 4
    assert (
        raw[footer_start_magic_offset : footer_start_magic_offset + 4] == b"PFA1"
    ), "footer-start magic must be PFA1"
    footer_json_bytes = raw[
        footer_start_magic_offset
        + 4 : footer_start_magic_offset
        + 4
        + footer_payload_length
    ]
    footer = json.loads(footer_json_bytes.decode("utf-8"))

    assert "blobs" in footer and isinstance(footer["blobs"], list) and footer["blobs"]
    blob = footer["blobs"][0]

    # Required keys per the Puffin spec; FileMetadataParser raises on any miss.
    for required in (
        "type",
        "fields",
        "snapshot-id",
        "sequence-number",
        "offset",
        "length",
    ):
        assert required in blob, (
            f"deletion-vector-v1 footer is MISSING required field '{required}' "
            f"({footer_json_bytes!r}). Spark's FileMetadataParser.blobMetadataFromJson "
            f"would reject this file with JsonUtil.getLong/getString."
        )

    # Spec-mandated DV v1 values.
    assert blob["type"] == "deletion-vector-v1"
    # The Puffin spec for deletion-vector-v1 requires `fields` to contain a
    # single entry: the row-position field id of `_pos` in the data file's
    # schema, which is Iceberg's reserved id 2147483546 (apache/iceberg
    # MetadataColumns.ROW_POSITION). Spark/Iceberg-Java rejects DV blobs
    # with an empty fields array.
    assert blob["fields"] == [
        2147483546
    ], "deletion-vector-v1 MUST have fields=[2147483546] (spec / Spark)"
    assert (
        blob["snapshot-id"] == -1
    ), "deletion-vector-v1 MUST have snapshot-id=-1 (spec)"
    assert (
        blob["sequence-number"] == -1
    ), "deletion-vector-v1 MUST have sequence-number=-1 (spec)"
    assert (
        "compression-codec" not in blob
    ), "deletion-vector-v1 MUST omit compression-codec (spec)"

    # The deletion-vector-v1 spec REQUIRES referenced-data-file + cardinality
    # in the blob's properties map (Spark uses them to attach the DV to a
    # data file in the manifest).
    props = blob["properties"]
    assert props["referenced-data-file"] == data_file
    assert int(props["cardinality"]) == len(positions)


def test_iceberg_write_dvs_large_cardinality(pgduck_conn, s3):
    """Encode/decode 10k positions and verify exact round-trip + size."""
    path = _puffin_path(s3, "large.puffin")
    data_file = f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv/data-large.parquet"
    n = 10_000
    # Build the integer literals in Python to avoid passing a subquery into
    # the table function (DuckDB rejects subqueries as table-function args).
    arr_sql = "ARRAY[" + ",".join(f"{i}::BIGINT" for i in range(1, n + 1)) + "]"

    write_row = run_query(
        f"""
        SELECT cardinality, content_size_in_bytes
        FROM iceberg_write_dvs('{path}', '{data_file}', {arr_sql})
        """,
        pgduck_conn,
    )
    cardinality, content_size = write_row[0]
    assert int(cardinality) == n
    # 10k dense low-32 ints compressed via Roaring should stay << 100KB.
    assert int(content_size) < 100_000

    md = run_query(
        f"""
        SELECT blob_offset, blob_length
        FROM iceberg_puffin_read_metadata('{path}') WHERE blob_idx = 0
        """,
        pgduck_conn,
    )
    blob_offset, blob_length = int(md[0][0]), int(md[0][1])

    # Round-trip via iceberg_dv_positions (array form, single-element arrays).
    decoded = run_query(
        f"""
        SELECT count(*) FROM iceberg_dv_positions(
            ['{path}'],
            [{blob_offset}::BIGINT],
            [{blob_length}::BIGINT],
            ['{data_file}']
        )
        """,
        pgduck_conn,
    )
    assert int(decoded[0][0]) == n
