"""
Regression tests for Avro container block-header validation.

An Avro container file stores, before each data block, a record count and a
compressed byte length, both as zigzag varints. libavro's
file_read_block_count() used them without checking their sign, so a corrupt or
misaligned file could steer the reader in two bad directions:

  * a negative length reached avro_malloc(len), and pg_lake routes libavro's
    allocator to palloc (see AvroPostgresAllocator in avro_init.c), so the
    negative value arrived as a size_t -- surfacing as
    "invalid memory alloc request size 18446744073709551563" for len == -53,
    an error that names neither the file nor the real problem;

  * or, once a block buffer already existed, a negative length is not greater
    than current_blocklen, so nothing was resized, the read and the codec
    decode were both skipped, and the block reader was left pointing at the
    *previous* block's decompressed bytes. The reader then re-emitted stale
    records and returned data that is not in the file, with no error at all.

Both are now rejected up front, so a damaged manifest is reported as damaged.
"""

import tempfile

import pytest
from utils_pytest import *


def _encode_zigzag_long(value):
    """Encode an int64 the way Avro's binary encoding does: zigzag + varint."""
    zigzag = (value << 1) ^ (value >> 63)
    zigzag &= (1 << 64) - 1

    out = bytearray()
    while True:
        chunk = zigzag & 0x7F
        zigzag >>= 7
        if zigzag:
            out.append(chunk | 0x80)
        else:
            out.append(chunk)
            return bytes(out)


def _decode_zigzag_long(data, offset):
    result = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        result |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            break
    return (result >> 1) ^ -(result & 1), offset


def _first_block_header_offset(data):
    """Return the offset just past the container header, where the first block
    header (count, length) begins: magic, the metadata map, then a 16-byte sync
    marker."""
    assert data[:4] == b"Obj\x01", "not an Avro container file"

    offset = 4
    while True:
        count, offset = _decode_zigzag_long(data, offset)
        if count == 0:
            break
        if count < 0:
            # a negative map block count is followed by its byte size
            _, offset = _decode_zigzag_long(data, offset)
            count = -count
        for _ in range(count):
            key_len, offset = _decode_zigzag_long(data, offset)
            offset += key_len
            value_len, offset = _decode_zigzag_long(data, offset)
            offset += value_len

    return offset + 16  # skip the sync marker


def _sync_marker(data):
    start = _first_block_header_offset(data) - 16
    return data[start : start + 16]


def _sample_manifest_bytes():
    with open(sample_avro_filepath("equality-ids-manifest.avro"), "rb") as f:
        return f.read()


def _read_manifest_error(manifest_url, superuser_conn):
    """Reserialize the manifest and return the error text, or None if it worked.

    run_query returns result rows on success and the error string on failure, so
    the type is what distinguishes the two.
    """
    with tempfile.NamedTemporaryFile(suffix=".avro") as out_file:
        result = run_query(
            f"SELECT lake_iceberg.reserialize_iceberg_manifest("
            f"'{manifest_url}', '{out_file.name}')",
            superuser_conn,
            raise_error=False,
        )

    return result if isinstance(result, str) else None


@pytest.fixture(scope="module")
def reserialize_functions(create_reserialize_helper_functions, superuser_conn):
    """Commit the helper function DDL.

    create_reserialize_helper_functions leaves it uncommitted, and every test
    here has to roll back after the failure it provokes, which would otherwise
    take the CREATE FUNCTION statements with it.
    """
    superuser_conn.commit()


def _upload(tmp_path, s3, key, payload):
    local = tmp_path / "manifest.avro"
    local.write_bytes(payload)
    s3.upload_file(str(local), TEST_BUCKET, key)
    return f"s3://{TEST_BUCKET}/{key}"


@pytest.mark.parametrize(
    "label,block_count,block_size",
    [
        ("negative_size", 1, -53),
        ("negative_count", -7, 84),
    ],
)
def test_first_block_header_with_negative_field_is_rejected(
    label,
    block_count,
    block_size,
    tmp_path,
    superuser_conn,
    s3,
    reserialize_functions,
):
    """A negative count or length in the *first* block header must fail the read.

    The first block is the case where no block buffer exists yet, so before the
    fix a negative length went straight to avro_malloc() and the statement died
    with "invalid memory alloc request size 18446744073709551563" instead of
    anything that identifies the file as corrupt.
    """
    original = _sample_manifest_bytes()
    header_end = _first_block_header_offset(original)

    corrupt = bytearray(original[:header_end])
    corrupt += _encode_zigzag_long(block_count)
    corrupt += _encode_zigzag_long(block_size)
    # Keep a plausible amount of trailing data so the failure has to come from
    # the block header itself and not from hitting end-of-file.
    corrupt += original[header_end:]

    url = _upload(tmp_path, s3, f"avro_block_header/{label}.avro", bytes(corrupt))
    error = _read_manifest_error(url, superuser_conn)
    superuser_conn.rollback()

    assert error is not None, (
        f"{label}: reading a manifest with a negative block header field "
        f"succeeded; file_read_block_count() must reject it"
    )
    assert "invalid memory alloc request size" not in str(
        error
    ), f"{label}: unvalidated block length reached the allocator: {error}"
    assert "Invalid file block" in str(
        error
    ), f"{label}: expected a block-header validation error, got: {error}"


def test_trailing_block_header_with_negative_size_does_not_return_stale_records(
    tmp_path, superuser_conn, s3, reserialize_functions
):
    """Garbage after the final sync marker must not be read as extra records.

    Appending a block header whose length is negative used to be silently
    accepted: no buffer was resized, the read and decode were skipped, and the
    block reader still pointed at the previous block's decompressed bytes, so
    the declared record count was satisfied from stale data. The manifest then
    read back with more entries than it contains -- in Iceberg terms, phantom
    data files -- and nothing reported a problem.
    """
    original = _sample_manifest_bytes()

    # A well-formed sync marker followed by a block header claiming 4 records of
    # negative length: the shape a partially-overwritten cache file produces.
    trailing = (
        _sync_marker(original)
        + _encode_zigzag_long(4)
        + _encode_zigzag_long(-53)
        + b"\x00" * 64
    )

    url = _upload(
        tmp_path, s3, "avro_block_header/stale_records.avro", original + trailing
    )
    error = _read_manifest_error(url, superuser_conn)
    superuser_conn.rollback()

    assert error is not None, (
        "a trailing block header with a negative length was accepted; the "
        "reader re-emitted the previous block's records instead of failing"
    )
    assert "Invalid file block size" in str(
        error
    ), f"expected a block-size validation error, got: {error}"
