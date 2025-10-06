# tests/test_fic_prefilter.py
import polars as pl
import pytest
from polars.testing import assert_frame_equal

# ⚠️ adjust this import to your project layout
from fic_prefilter import apply_fic_prefilter

def lf(df: pl.DataFrame) -> pl.LazyFrame:
    return df.lazy()

def collect_sorted(lf_: pl.LazyFrame, by: str) -> pl.DataFrame:
    return lf_.collect().sort(by)

def test_returns_lazyframes_and_noop_when_fic_pairs_empty():
    left_in  = pl.DataFrame({"left_id": [1, 2, 3], "x": ["a", "b", "c"]})
    right_in = pl.DataFrame({"right_id": [10, 20], "y": [0.1, 0.2]})
    fic_pairs = pl.DataFrame({"intern_id": [], "target_id": []})

    left_out_lf, right_out_lf = apply_fic_prefilter(
        lf(left_in), lf(right_in), fic_pairs, left_id="left_id", right_id="right_id"
    )

    assert isinstance(left_out_lf, pl.LazyFrame)
    assert isinstance(right_out_lf, pl.LazyFrame)

    assert_frame_equal(collect_sorted(left_out_lf, "left_id"), left_in.sort("left_id"))
    assert_frame_equal(collect_sorted(right_out_lf, "right_id"), right_in.sort("right_id"))

def test_removes_ids_present_in_fic_pairs_both_sides():
    left_in  = pl.DataFrame({"lid": [1, 2, 3, 4], "x": ["a", "b", "c", "d"]})
    right_in = pl.DataFrame({"rid": [10, 20, 30], "y": [0.1, 0.2, 0.3]})
    # 2 must be removed from left, 20 & 99 requested on right but 99 not present (no-op)
    fic_pairs = pl.DataFrame({"intern_id": [2, 2, 999], "target_id": [20, 99, 20]})

    left_out_lf, right_out_lf = apply_fic_prefilter(
        lf(left_in), lf(right_in), fic_pairs, left_id="lid", right_id="rid"
    )

    expected_left  = pl.DataFrame({"lid": [1, 3, 4], "x": ["a", "c", "d"]}).sort("lid")
    expected_right = pl.DataFrame({"rid": [10, 30], "y": [0.1, 0.3]}).sort("rid")

    assert_frame_equal(collect_sorted(left_out_lf, "lid"), expected_left)
    assert_frame_equal(collect_sorted(right_out_lf, "rid"), expected_right)

def test_no_overlap_means_no_change():
    left_in  = pl.DataFrame({"id_left": [1, 2], "val": ["u", "v"]})
    right_in = pl.DataFrame({"id_right": [10, 20], "val": ["p", "q"]})
    fic_pairs = pl.DataFrame({"intern_id": [999], "target_id": [888]})

    left_out_lf, right_out_lf = apply_fic_prefilter(
        lf(left_in), lf(right_in), fic_pairs, left_id="id_left", right_id="id_right"
    )

    assert_frame_equal(collect_sorted(left_out_lf, "id_left"), left_in.sort("id_left"))
    assert_frame_equal(collect_sorted(right_out_lf, "id_right"), right_in.sort("id_right"))

def test_duplicates_in_fic_pairs_do_not_double_filter():
    left_in  = pl.DataFrame({"L": [1, 2, 3, 4]})
    right_in = pl.DataFrame({"R": [10, 20, 30, 40]})
    # same id repeated many times; result should remove it once
    fic_pairs = pl.DataFrame({"intern_id": [1, 1, 1, 1], "target_id": [20, 20, 20, 20]})

    left_out_lf, right_out_lf = apply_fic_prefilter(
        lf(left_in), lf(right_in), fic_pairs, left_id="L", right_id="R"
    )

    expected_left  = pl.DataFrame({"L": [2, 3, 4]}).sort("L")
    expected_right = pl.DataFrame({"R": [10, 30, 40]}).sort("R")

    assert_frame_equal(collect_sorted(left_out_lf, "L"), expected_left)
    assert_frame_equal(collect_sorted(right_out_lf, "R"), expected_right)

@pytest.mark.parametrize(
    "left_col,right_col",
    [
        ("id", "id"),      # same name on both sides
        ("a_left", "b_r"), # totally different names
    ],
)
def test_custom_id_column_names_are_respected(left_col, right_col):
    left_in  = pl.DataFrame({left_col: [1, 2, 3], "x": [0, 0, 0]})
    right_in = pl.DataFrame({right_col: [10, 20, 30], "y": [1, 1, 1]})
    fic_pairs = pl.DataFrame({"intern_id": [3], "target_id": [10]})

    left_out_lf, right_out_lf = apply_fic_prefilter(
        lf(left_in), lf(right_in), fic_pairs, left_id=left_col, right_id=right_col
    )

    expected_left  = left_in.filter(pl.col(left_col) != 3).sort(left_col)
    expected_right = right_in.filter(pl.col(right_col) != 10).sort(right_col)

    assert_frame_equal(collect_sorted(left_out_lf, left_col), expected_left)
    assert_frame_equal(collect_sorted(right_out_lf, right_col), expected_right)
