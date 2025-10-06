import pytest
import polars as pl
from pathlib import Path
from rules_loader import (
    MatchingRule,
    RuleSet,
    _infer_kind,
    _level_sort_key,
    _validate_rule,
    _deep_merge,
    _load_yaml_file,
    _parse_rules,
    load_rules_for_provider,
)

# ==============================================================
# Helpers
# ==============================================================

def make_rule():
    return MatchingRule(
        level="Level1",
        name="Test Rule",
        rmpm_cols=["a", "b"],
        provider_cols=["x", "y"],
        kind="exact",
        cardinality=2,
    )

# ==============================================================
# MatchingRule & RuleSet
# ==============================================================

def test_matching_rule_to_dict():
    r = make_rule()
    d = r.to_dict()
    assert d["level"] == "Level1"
    assert d["cardinality"] == 2
    assert "rmpm" in d and "provider" in d


def test_ruleset_methods():
    r = make_rule()
    rs = RuleSet(provider="prov", rules=[r], schema={"a": pl.Utf8})
    assert rs.by_kind("exact")[0] == r
    assert rs.by_level("Level1") == r
    assert rs.by_level("unknown") is None
    d = rs.to_dict()
    assert d["provider"] == "prov"
    assert isinstance(d["rules"], list)

# ==============================================================
# Utils
# ==============================================================

def test_infer_kind():
    assert _infer_kind("level1", "fuzzy rule") == "fuzzy"
    assert _infer_kind("level1", "something") == "exact"


def test_level_sort_key():
    assert _level_sort_key("Level2") < _level_sort_key("Fuzzy")
    assert _level_sort_key("Fuzzy")[0] == 1


def test_validate_rule_ok():
    raw = {"matching_rule": "MyRule", "rmpm": ["a"], "provider": ["x"]}
    rule = _validate_rule("Level1", raw)
    assert rule.kind == "exact"
    assert rule.cardinality == 1


@pytest.mark.parametrize("bad_raw", [
    {},  # no matching_rule
    {"matching_rule": "x"},  # no rmpm/provider
    {"matching_rule": "x", "rmpm": [], "provider": ["a"]},  # empty lists
    {"matching_rule": "x", "rmpm": ["a"], "provider": []},  # empty provider
    {"matching_rule": "x", "rmpm": ["a"], "provider": ["x", "y"]},  # diff len
])
def test_validate_rule_errors(bad_raw):
    with pytest.raises(ValueError):
        _validate_rule("LevelX", bad_raw)


def test_deep_merge_dict_and_list():
    base = {"a": 1, "b": {"x": 10}}
    override = {"b": {"x": 20}, "c": 3}
    result = _deep_merge(base, override)
    assert result["b"]["x"] == 20
    assert result["c"] == 3

def test_deep_merge_non_dict():
    assert _deep_merge(1, 2) == 2

# ==============================================================
# YAML + Parsing
# ==============================================================

def test_load_yaml_file(tmp_path):
    path = tmp_path / "test.yaml"
    path.write_text("x: 1")
    assert _load_yaml_file(path)["x"] == 1


def test_parse_rules_and_sort_ok(tmp_path):
    data = {
        "matching_criterias": {
            "Level2": {"matching_rule": "R2", "rmpm": ["a"], "provider": ["b"]},
            "Level1": {"matching_rule": "R1", "rmpm": ["a"], "provider": ["b"]},
        }
    }
    rules = _parse_rules(data)
    assert [r.level for r in rules] == ["Level1", "Level2"]


@pytest.mark.parametrize("bad_data", [
    {},  # missing matching_criterias
    {"matching_criterias": []},  # not dict
    {"matching_criterias": {"Level1": "not a dict"}},  # wrong type
])
def test_parse_rules_errors(bad_data):
    with pytest.raises(ValueError):
        _parse_rules(bad_data)

# ==============================================================
# load_rules_for_provider
# ==============================================================

def test_load_rules_for_provider_base_only(tmp_path):
    base_yaml = tmp_path / "base.yaml"
    base_yaml.write_text("""
matching_criterias:
  Level1:
    matching_rule: "Test"
    rmpm: ["a"]
    provider: ["b"]
schema:
  col1: Utf8
""")
    rs = load_rules_for_provider("unknown", base_dir=tmp_path)
    assert isinstance(rs, RuleSet)
    assert rs.provider == "base"
    assert list(rs.schema.keys()) == ["col1"]


def test_load_rules_for_provider_with_override(tmp_path):
    base_yaml = tmp_path / "base.yaml"
    prov_yaml = tmp_path / "prov.yaml"
    base_yaml.write_text("""
matching_criterias:
  Level1:
    matching_rule: "Base"
    rmpm: ["a"]
    provider: ["b"]
""")
    prov_yaml.write_text("""
matching_criterias:
  Level1:
    matching_rule: "Override"
    rmpm: ["x"]
    provider: ["y"]
schema:
  col1: Utf8
""")
    rs = load_rules_for_provider("prov", base_dir=tmp_path)
    assert rs.provider == "prov"
    assert rs.rules[0].name == "Override"


def test_load_rules_for_provider_missing_base(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_rules_for_provider("prov", base_dir=tmp_path)
