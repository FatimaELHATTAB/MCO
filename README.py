from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import yaml
import re
from pathlib import Path


# ============================================================
#        Data models (inchangés, + helpers)
# ============================================================

@dataclass(frozen=True)
class MatchingRule:
    level: str                     # ex: "Level1", "Fuzzy"
    name: str                      # ex: "3WM Legal Name, Immat Country, LEI"
    mpm_cols: List[str]            # colonnes côté MPM
    provider_cols: List[str]       # colonnes côté Provider
    kind: str                      # "exact" | "fuzzy"
    cardinality: int               # len(mpm_cols)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "level": self.level,
            "name": self.name,
            "mpm": list(self.mpm_cols),
            "provider": list(self.provider_cols),
            "kind": self.kind,
            "cardinality": self.cardinality,
        }


@dataclass(frozen=True)
class RuleSet:
    provider: str                  # nom du provider chargé (ou "base")
    rules: List[MatchingRule]

    def by_kind(self, kind: str) -> List[MatchingRule]:
        return [r for r in self.rules if r.kind == kind]

    def by_level(self, level: str) -> Optional[MatchingRule]:
        for r in self.rules:
            if r.level == level:
                return r
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "provider": self.provider,
            "rules": [r.to_dict() for r in self.rules]
        }


# ============================================================
#        Utils: merge YAML, parse & validate
# ============================================================

def _infer_kind(level: str, name: str) -> str:
    """Devine 'exact' vs 'fuzzy' à partir du level/nom."""
    text = f"{level} {name}".lower()
    return "fuzzy" if "fuzzy" in text else "exact"

def _level_sort_key(level: str) -> tuple:
    """
    Trie 'LevelN' dans l'ordre numérique; 'Fuzzy' (ou labels non-numériques) après.
    Ex: Level1 < Level2 < ... < Fuzzy
    """
    m = re.match(r"level\s*(\d+)", level.strip(), flags=re.IGNORECASE)
    if m:
        return (0, int(m.group(1)))
    return (1, level.lower())

def _validate_rule(level: str, raw: Dict[str, Any]) -> MatchingRule:
    if "matching_rule" not in raw:
        raise ValueError(f"[{level}] Champ 'matching_rule' manquant")
    if "mpm" not in raw or "provider" not in raw:
        raise ValueError(f"[{level}] Champs 'mpm' et/ou 'provider' manquants")

    name = str(raw["matching_rule"])
    mpm_cols = list(raw["mpm"])
    provider_cols = list(raw["provider"])

    if not mpm_cols or not provider_cols:
        raise ValueError(f"[{level}] Listes 'mpm'/'provider' vides")
    if len(mpm_cols) != len(provider_cols):
        raise ValueError(
            f"[{level}] Longueurs différentes: mpm({len(mpm_cols)}) != provider({len(provider_cols)})"
        )

    kind = _infer_kind(level, name)
    cardinality = len(mpm_cols)

    return MatchingRule(
        level=level,
        name=name,
        mpm_cols=mpm_cols,
        provider_cols=provider_cols,
        kind=kind,
        cardinality=cardinality,
    )

def _deep_merge(base: Any, override: Any) -> Any:
    """
    Deep merge:
      - dict: fusion clé par clé (override écrase base pour scalaires/listes)
      - list/scalaires: override remplace base
    """
    if isinstance(base, dict) and isinstance(override, dict):
        out = dict(base)
        for k, v in override.items():
            if k in out:
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out
    # par défaut: override gagne (listes, scalaires…)
    return override

def _load_yaml_file(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def _parse_rules(mapping: Dict[str, Any]) -> List[MatchingRule]:
    if "matching_criterias" not in mapping or not isinstance(mapping["matching_criterias"], dict):
        raise ValueError("YAML invalide: clé 'matching_criterias' manquante ou non-mapping")
    mc = mapping["matching_criterias"]
    parsed: List[MatchingRule] = []
    for level, raw_rule in mc.items():
        if not isinstance(raw_rule, dict):
            raise ValueError(f"[{level}] la valeur doit être un mapping")
        parsed.append(_validate_rule(level, raw_rule))
    parsed_sorted = sorted(parsed, key=lambda r: _level_sort_key(r.level))
    return parsed_sorted


# ============================================================
#        API principale: chargement par provider
# ============================================================

def load_rules_for_provider(
    provider: str,
    base_dir: str | Path = "config/providers",
    base_filename: str = "base.yaml",
) -> RuleSet:
    """
    Charge les règles pour un provider donné, avec héritage:
      1) charge base.yaml
      2) si {provider}.yaml existe, deep-merge par-dessus base
      3) sinon, fallback: uniquement base
    Retourne un RuleSet (provider = nom réel chargé ou 'base' si fallback).
    """
    base_dir = Path(base_dir)
    base_path = base_dir / base_filename
    prov_path = base_dir / f"{provider}.yaml"

    if not base_path.exists():
        raise FileNotFoundError(f"Fichier base introuvable: {base_path}")

    base_cfg = _load_yaml_file(base_path)

    if prov_path.exists():
        prov_cfg = _load_yaml_file(prov_path)
        merged = _deep_merge(base_cfg, prov_cfg)
        rules = _parse_rules(merged)
        return RuleSet(provider=provider, rules=rules)
    else:
        # fallback sur base
        rules = _parse_rules(base_cfg)
        return RuleSet(provider="base", rules=rules)



from pathlib import Path
import yaml
from matching_engine.core.rules_loader import load_rules_for_provider, RuleSet

def _write_yaml(path: Path, data: dict):
    path.write_text(yaml.safe_dump(data, sort_keys=False, allow_unicode=True), encoding="utf-8")

def test_load_base_only(tmp_path):
    providers_dir = tmp_path / "config" / "providers"
    providers_dir.mkdir(parents=True)
    base = {
        "matching_criterias": {
            "Level1": {
                "matching_rule": "3WM A,B,C",
                "mpm": ["a","b","c"],
                "provider": ["aa","bb","cc"]
            },
            "Fuzzy": {
                "matching_rule": "Fuzzy A,B",
                "mpm": ["a_clean","b"],
                "provider": ["aa_clean","bb"]
            }
        }
    }
    _write_yaml(providers_dir / "base.yaml", base)

    rs: RuleSet = load_rules_for_provider("unknown", base_dir=providers_dir)
    assert rs.provider == "base"
    levels = [r.level for r in rs.rules]
    assert levels == ["Level1", "Fuzzy"]

def test_load_provider_inherits_and_overrides(tmp_path):
    providers_dir = tmp_path / "config" / "providers"
    providers_dir.mkdir(parents=True)
    base = {
        "matching_criterias": {
            "Level1": {
                "matching_rule": "3WM A,B,C",
                "mpm": ["a","b","c"],
                "provider": ["aa","bb","cc"]
            },
            "Level5": {
                "matching_rule": "2WM A,B",
                "mpm": ["a","b"],
                "provider": ["aa","bb"]
            },
            "Fuzzy": {
                "matching_rule": "Fuzzy A,B",
                "mpm": ["a_clean","b"],
                "provider": ["aa_clean","bb"]
            }
        }
    }
    prov = {
        # override du nom de règle et des colonnes pour Level5
        "matching_criterias": {
            "Level5": {
                "matching_rule": "2WM X,Y",
                "mpm": ["x","y"],
                "provider": ["xx","yy"]
            },
            # ajout d'un Level10 spécifique provider
            "Level10": {
                "matching_rule": "1WM Z",
                "mpm": ["z"],
                "provider": ["zz"]
            }
        }
    }
    _write_yaml(providers_dir / "base.yaml", base)
    _write_yaml(providers_dir / "bloomberg.yaml", prov)

    rs: RuleSet = load_rules_for_provider("bloomberg", base_dir=providers_dir)
    assert rs.provider == "bloomberg"

    # vérifie tri & override
    levels = [r.level for r in rs.rules]
    # Level1 (base) < Level5 (override) < Level10 (provider) < Fuzzy (base)
    assert levels == ["Level1", "Level5", "Level10", "Fuzzy"]

    # Level5 doit venir du provider (override)
    lvl5 = rs.by_level("Level5")
    assert lvl5 and lvl5.name == "2WM X,Y"
    assert lvl5.mpm_cols == ["x","y"]
    assert lvl5.provider_cols == ["xx","yy"]

    # Fuzzy reste présent depuis base
    fuzzy = rs.by_level("Fuzzy")
    assert fuzzy and fuzzy.kind == "fuzzy"

