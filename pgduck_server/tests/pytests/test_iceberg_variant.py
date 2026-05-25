"""
Phase 1 + 1.5 of the Iceberg V3 Variant POC.

This file lives at the pgduck_server level (not pg_lake_iceberg) because every
assertion here is about DuckDB's own VARIANT machinery: cast round-trips,
parquet write/read shape, operator binding, and the shape of pushdown plans.

Findings the assertions enforce (see plan
docs/iceberg_v3_variant_poc_+_pg_lake_plumbing.plan.md for context):

  * JSON <-> VARIANT cast round-trips losslessly. This is the data path the
    pg_lake SQL builder will rely on.
  * variant_extract / [i] return the inner value directly.
  * ->'k' and ->>'k' are NOT bound for VARIANT in DuckDB v1.5.1: they fall
    through to JSON-extension overloads that implicit-cast VARIANT->VARCHAR
    and then choke on the resulting string. The Phase 2 patch adds VARIANT
    overloads. Until then these are pinned here as expected failures so we
    catch the day DuckDB starts handling them natively.
  * Default parquet write of VARIANT emits only metadata + value BLOB columns;
    shredded `typed_value` children are opt-in via a COPY (shredding {...})
    schema. We assert the unshredded shape so we notice if a future bump
    flips the default.
  * `variant_extract(col, 'k') = ...` does push into READ_PARQUET as a Filter
    (not as a post-scan projection). The Filter sits on the variant value so
    without typed_value columns it can't be vector-prefiltered, but the
    predicate pushdown surface IS in place for future shredded files.

Run with:
  PYTHONPATH=../test_common pipenv run pytest -v tests/pytests/test_iceberg_variant.py
"""

import os
import json
import pytest
import duckdb


JSON_DOC_A = '{"a":1,"b":"x","nested":{"k":"v"}}'
JSON_DOC_B = '{"a":2,"b":"y"}'


@pytest.fixture
def con():
    """A standalone DuckDB connection. We deliberately avoid pgduck_server
    because every assertion here is DuckDB-internal and going through the
    PG wire protocol would only obscure error messages."""
    c = duckdb.connect()
    yield c
    c.close()


@pytest.fixture
def variant_parquet(con, tmp_path):
    """Two-row parquet with a VARIANT column written via the actual
    pg_lake-style chain: literal JSON -> CAST AS VARIANT -> COPY."""
    p = str(tmp_path / "variant.parquet")
    con.execute("CREATE TEMP TABLE t (col VARIANT)")
    con.execute(f"INSERT INTO t VALUES (CAST('{JSON_DOC_A}'::JSON AS VARIANT))")
    con.execute(f"INSERT INTO t VALUES (CAST('{JSON_DOC_B}'::JSON AS VARIANT))")
    con.execute(f"COPY t TO '{p}'")
    assert os.path.getsize(p) > 0
    return p


# ---------------------------------------------------------------------------
# Phase 1 - standalone POC
# ---------------------------------------------------------------------------


class TestVariantCastRoundtrip:
    """The conversion-layer premise: JSONB-as-VARIANT must be lossless on the
    DuckDB side."""

    def test_json_to_variant_to_json_via_select(self, con):
        # Round trip through the cast pair the SQL builders will emit.
        r = con.execute(
            f"SELECT CAST(CAST('{JSON_DOC_A}'::JSON AS VARIANT) AS JSON) AS j"
        ).fetchall()
        assert len(r) == 1
        # Whitespace / key-order are not guaranteed; compare parsed.
        assert json.loads(r[0][0]) == json.loads(JSON_DOC_A)

    def test_malformed_json_to_variant_errors(self, con):
        # Negative path: cast must fail loudly, not silently corrupt.
        with pytest.raises(duckdb.Error):
            con.execute("SELECT CAST('not-json'::JSON AS VARIANT)").fetchall()

    def test_variant_extract_returns_inner_value(self, con):
        r = con.execute(
            f"SELECT variant_extract(CAST('{JSON_DOC_A}'::JSON AS VARIANT), 'a')"
        ).fetchall()
        # variant_extract returns VARIANT; the integer is unwrapped on display.
        assert r[0][0] == 1

    def test_bracket_extract_works_on_variant(self, con):
        # `[i]` is rewritten to variant_extract at bind time
        # (bind_operator_expression.cpp:138-148).
        r = con.execute(
            f"SELECT (CAST('{JSON_DOC_A}'::JSON AS VARIANT))['a']"
        ).fetchall()
        assert r[0][0] == 1


class TestVariantParquetRoundtrip:
    """Phase 1 end-to-end on the parquet boundary, mirroring what the iceberg
    SQL builder will emit on writes (CAST input AS VARIANT) and reads
    (CAST col AS JSON)."""

    def test_read_back_via_cast_as_json(self, con, variant_parquet):
        r = con.execute(
            f"SELECT CAST(col AS JSON) FROM read_parquet('{variant_parquet}') ORDER BY 1"
        ).fetchall()
        assert len(r) == 2
        # Reconstruct the input set; key-order is not guaranteed. We compare
        # by parsing each row and asserting both expected docs are present.
        got = [json.loads(row[0]) for row in r]
        want = [json.loads(JSON_DOC_A), json.loads(JSON_DOC_B)]
        assert len(got) == len(want)
        for w in want:
            assert w in got, f"missing {w} in {got}"


# ---------------------------------------------------------------------------
# Phase 1.5 - probes
# ---------------------------------------------------------------------------


class TestProbeBWriteShape:
    """Probe B: what does DuckDB v1.5.1 emit for VARIANT into parquet at
    default settings?

    Findings encoded as assertions:
      * The column has 2 children: `metadata` (BLOB) and `value` (BLOB).
      * No `typed_value` children -> shredding is NOT automatic.
      * `variant_minimum_shredding_size` is a checkpoint setting and does not
        flip parquet writes; only the explicit `COPY ... (shredding {...})`
        option produces typed_value subcolumns.
    """

    def test_default_write_is_unshredded(self, con, variant_parquet):
        rows = con.execute(
            f"""
            SELECT name, type, num_children
            FROM parquet_schema('{variant_parquet}')
            ORDER BY column_id
            """
        ).fetchall()
        # rows: schema root, col, metadata, value
        names = [r[0] for r in rows]
        assert names == ["duckdb_schema", "col", "metadata", "value"]
        # `col` is a struct of 2 children (metadata+value), nothing typed_*.
        col_row = next(r for r in rows if r[0] == "col")
        assert col_row[2] == 2, "default write must be unshredded (2 children)"
        for child in ("metadata", "value"):
            child_row = next(r for r in rows if r[0] == child)
            assert child_row[1] == "BYTE_ARRAY"

    def test_lowering_threshold_does_not_enable_shredded_parquet(self, con, tmp_path):
        """Sanity: variant_minimum_shredding_size is a checkpoint knob, not
        a parquet writer knob. Lowering it must not produce typed_value cols."""
        con.execute("SET variant_minimum_shredding_size = 0")
        p = str(tmp_path / "low.parquet")
        con.execute("CREATE TEMP TABLE t2 (col VARIANT)")
        for i in range(50):
            con.execute(
                f"INSERT INTO t2 VALUES "
                f'(CAST(\'{{"a":{i},"b":"x{i}"}}\'::JSON AS VARIANT))'
            )
        con.execute(f"COPY t2 TO '{p}'")
        names = [
            r[0]
            for r in con.execute(f"SELECT name FROM parquet_schema('{p}')").fetchall()
        ]
        assert (
            "typed_value" not in names
        ), "If this fails, DuckDB started auto-shredding; revisit the plan."
        con.execute("RESET variant_minimum_shredding_size")

    def test_explicit_shredding_option_produces_typed_value(self, con, tmp_path):
        """Pinning down the OPT-IN path: with an explicit shredding schema
        the writer DOES emit typed_value children. This is the path that
        future pg_lake versions could plug into; out of scope for this POC."""
        p = str(tmp_path / "shredded.parquet")
        con.execute(
            f"""
            COPY (
                SELECT CAST('{JSON_DOC_A}'::JSON AS VARIANT) AS col
                UNION ALL
                SELECT CAST('{JSON_DOC_B}'::JSON AS VARIANT)
            ) TO '{p}' (
                shredding {{
                    col: 'STRUCT(a INTEGER, b VARCHAR)'
                }}
            )
            """
        )
        names = [
            r[0]
            for r in con.execute(f"SELECT name FROM parquet_schema('{p}')").fetchall()
        ]
        # With explicit shredding, we expect typed_value subcolumns alongside
        # metadata + value.
        assert "typed_value" in names


class TestProbeAOperatorBinding:
    """Probe A: can pg_lake's existing JSONB pushdown -> DuckDB's parser
    survive a VARIANT-typed source column?

    Source-code reading (bind_operator_expression.cpp:138-148) suggested `[i]`
    auto-rewrites to variant_extract; everything else falls through to JSON
    extension name aliases.

    Empirical: BOTH `->` and `->>` are JSON-extension aliases with no VARIANT
    overload, so they go through implicit VARIANT->VARCHAR cast and crash on
    "Malformed JSON". Phase 2 adds VARIANT overloads so existing pushdown
    keeps working.
    """

    def test_arrow_op_currently_crashes_on_variant(self, con, variant_parquet):
        """Pin the today-broken behavior so a future fix is detected."""
        with pytest.raises(duckdb.Error) as ei:
            con.execute(
                f"SELECT col->'a' FROM read_parquet('{variant_parquet}')"
            ).fetchall()
        assert "Malformed JSON" in str(ei.value) or "Cannot extract" in str(ei.value)

    def test_arrow_string_op_currently_crashes_on_variant(self, con, variant_parquet):
        with pytest.raises(duckdb.Error) as ei:
            con.execute(
                f"SELECT col->>'a' FROM read_parquet('{variant_parquet}')"
            ).fetchall()
        assert "Malformed JSON" in str(ei.value) or "Cannot extract" in str(ei.value)

    def test_no_variant_overload_for_arrow_operators_today(self, con):
        """Function-catalog level confirmation: ->, ->> have no VARIANT row.
        If this changes upstream we want the Phase 2 patch to no-op."""
        rows = con.execute(
            """
            SELECT function_name, parameter_types
            FROM duckdb_functions()
            WHERE function_name IN ('->', '->>', 'json_extract', 'json_extract_string')
            """
        ).fetchall()
        for _name, params in rows:
            assert (
                "VARIANT" not in params
            ), "DuckDB now has a VARIANT overload for arrow ops; revisit Phase 2."

    def test_variant_extract_predicate_lands_in_read_parquet_filter(
        self, con, variant_parquet
    ):
        """The pushdown surface: predicates wrapping variant_extract must show
        up as a READ_PARQUET filter, not as a post-scan projection. This is
        the half of the perf story that does survive in v1.5.1; the other half
        (shredded `typed_value` lookup) needs Probe B's opt-in path."""
        plan = con.execute(
            f"""
            EXPLAIN SELECT count(*)
            FROM read_parquet('{variant_parquet}')
            WHERE variant_extract(col, 'a')::INTEGER = 1
            """
        ).fetchall()
        plan_text = "\n".join(row[1] if len(row) > 1 else row[0] for row in plan)
        assert "READ_PARQUET" in plan_text
        # The filter expression must appear in the READ_PARQUET node, not
        # above it as a separate Filter operator.
        assert "variant_extract" in plan_text
        assert "Filters:" in plan_text


class TestVariantExtractPushdownOnTopOfReadParquet:
    """Belt-and-suspenders: confirm variant_extract executes correctly so a
    Phase 2 ->> rewrite to variant_extract is functionally equivalent to PG's
    JSONB ->>."""

    def test_variant_extract_filter_returns_correct_count(self, con, variant_parquet):
        r = con.execute(
            f"""
            SELECT count(*)::INT
            FROM read_parquet('{variant_parquet}')
            WHERE variant_extract(col, 'a')::INTEGER = 1
            """
        ).fetchall()
        assert r[0][0] == 1

    def test_variant_extract_string_value(self, con, variant_parquet):
        r = con.execute(
            f"""
            SELECT count(*)::INT
            FROM read_parquet('{variant_parquet}')
            WHERE variant_extract(col, 'b')::VARCHAR = 'x'
            """
        ).fetchall()
        assert r[0][0] == 1
