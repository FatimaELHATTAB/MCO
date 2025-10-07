import io
from pathlib import Path
import polars as pl
import yaml
import pytest

from data_config import load_data_io_config, DataIOConfig


def write_yaml(tmp_path: Path, content: dict) -> Path:
    p = tmp_path / "data.yaml"
    p.write_text(yaml.safe_dump(content, sort_keys=False), encoding="utf-8")
    return p


def test_full_config_parsing(tmp_path: Path):
    content = {
        "database": {
            "table": "rmpm_table",
            "base_condition": "is_active = true",
            "country_condition": "country IN ('FR','IT')",
        },
        "database_fic": {
            "table": "fic_rmpm",
            "base_condition": "1=1",
        },
        "ids": {"left": "rmpm_id", "right": "provider_id"},
        "options": {
            "distinct": False,
            "limit_left": 100,
            "limit_right": 200,
            "country_scope": ["FR", "IT"],
        },
        "schema": {"id": "Int64", "name": "Utf8", "is_ok": "Boolean"},
    }
    path = write_yaml(tmp_path, content)

    cfg = load_data_io_config(path)

    assert isinstance(cfg, DataIOConfig)
    assert cfg.table == "rmpm_table"
    assert cfg.base_condition == "is_active = true"
    assert cfg.country_condition == "country IN ('FR','IT')"

    assert cfg.left_id == "rmpm_id"
    assert cfg.right_id == "provider_id"
    assert cfg.distinct is False
    assert cfg.limit_left == 100
    assert cfg.limit_right == 200
    assert cfg.fic_table == "fic_rmpm"
    assert cfg.fic_base_condition == "1=1"
    assert cfg.country_scope == ["FR", "IT"]

    # types Polars correctement construits
    assert cfg.schema["id"] is pl.Int64
    assert cfg.schema["name"] is pl.Utf8
    assert cfg.schema["is_ok"] is pl.Boolean


def test_defaults_when_options_missing(tmp_path: Path):
    content = {
        "database": {"table": "t", "base_condition": "1=1", "country_condition": "1=1"},
        "database_fic": {"table": "t2", "base_condition": "1=1"},
        "ids": {"left": "L", "right": "R"},
        "schema": {"x": "Int64"},
        # options intentionally omitted
    }
    path = write_yaml(tmp_path, content)

    cfg = load_data_io_config(path)

    assert cfg.distinct is True  # défaut
    assert cfg.limit_left is None
    assert cfg.limit_right is None
    assert cfg.country_scope == []


def test_handles_empty_file_gracefully(tmp_path: Path):
    # Fichier YAML vide -> tout par défaut / chaînes vides / None
    path = tmp_path / "empty.yaml"
    path.write_text("", encoding="utf-8")

    cfg = load_data_io_config(path)

    assert cfg.table == ""
    assert cfg.fic_table == ""
    assert cfg.left_id == ""
    assert cfg.right_id == ""
    assert cfg.schema == {}
    assert cfg.limit_left is None and cfg.limit_right is None
    assert cfg.country_scope == []
