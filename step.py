from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import yaml
import re


# ---------------------------
# Modèles de données simples
# ---------------------------

@dataclass(frozen=True)
class MatchingRule:
    level: str                     # ex: "Level1", "Fuzzy"
    name: str                      # ex: "3WM Legal Name, Immat Country, LEI"
    mpm_cols: List[str]            # colonnes côté MPM
    provider_cols: List[str]       # colonnes côté Provider
    kind: str                      # "exact" | "fuzzy"
    cardinality: int               # 1, 2, 3... (= len(mpm_cols))

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
    rules: List[MatchingRule]

    def by_kind(self, kind: str) -> List[MatchingRule]:
        return [r for r in self.rules if r.kind == kind]

    def by_level(self, level: str) -> Optional[MatchingRule]:
        for r in self.rules:
            if r.level == level:
                return r
        return None

    def to_dict(self) -> Dict[str, Any]:
        return {"rules": [r.to_dict() for r in self.rules]}


# ---------------------------
# Parsing & validation
# ---------------------------

def _infer_kind(level: str, name: str) -> str:
    """Devine 'exact' vs 'fuzzy' à partir du level/nom."""
    text = f"{level} {name}".lower()
    return "fuzzy" if "fuzzy" in text else "exact"

def _level_sort_key(level: str) -> tuple:
    """
    Trie 'LevelN' dans l'ordre numérique, garde 'Fuzzy' à la fin.
    Ex: Level1 < Level2 < ... < Fuzzy
    """
    m = re.match(r"level\s*(\d+)", level.strip(), flags=re.IGNORECASE)
    if m:
        return (0, int(m.group(1)))
    # Fuzzy ou autres labels : après les LevelN
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


# ---------------------------
# API principale du module
# ---------------------------

def load_rules_from_path(path: str) -> RuleSet:
    """
    Charge un YAML contenant 'matching_criterias' et renvoie un RuleSet.
    Le YAML minimal attendu :
      matching_criterias:
        Level1:
          matching_rule: "3WM ..."
          mpm: [ ... ]
          provider: [ ... ]
        Fuzzy:
          matching_rule: "Fuzzy ..."
          mpm: [ ... ]
          provider: [ ... ]
    """
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict) or "matching_criterias" not in data:
        raise ValueError("YAML invalide: la clé racine 'matching_criterias' est requise")

    mc = data["matching_criterias"]
    if not isinstance(mc, dict) or not mc:
        raise ValueError("'matching_criterias' doit être un mapping non vide")

    parsed: List[MatchingRule] = []
    for level, raw_rule in mc.items():
        if not isinstance(raw_rule, dict):
            raise ValueError(f"[{level}] la valeur doit être un mapping")
        parsed.append(_validate_rule(level, raw_rule))

    # Tri stable: Level1 < Level2 < ... < Fuzzy (ou labels non numériques)
    parsed_sorted = sorted(parsed, key=lambda r: _level_sort_key(r.level))
    return RuleSet(rules=parsed_sorted)




import io
import yaml
from matching_engine.core.rules_loader import load_rules_from_path, RuleSet, MatchingRule

def _write_tmp(tmp_path, text: str) -> str:
    p = tmp_path / "rules.yaml"
    p.write_text(text, encoding="utf-8")
    return str(p)

def test_load_minimal_levels(tmp_path):
    yml = """
matching_criterias:
  Level1:
    matching_rule: "3WM Legal Name, Immat Country, LEI"
    mpm:      [ "LB_RAISON_SOCIAL_left_untouched", "CD_PAYS_IMMAT_left", "LEI_final" ]
    provider: [ "TGT_COMPANY_NAME_untouched",      "TGT_COUNTRY",        "LEI" ]

  Level5:
    matching_rule: "2WM Legal Name, Immat Country"
    mpm:      [ "LB_RAISON_SOCIAL_left_original", "CD_PAYS_IMMAT_left" ]
    provider: [ "TGT_COMPANY_NAME_original",      "TGT_COUNTRY" ]

  Fuzzy:
    matching_rule: "Fuzzy Legal Name, Immat Country"
    mpm:      [ "LB_RAISON_SOCIAL_left_cleaned", "CD_PAYS_IMMAT_left" ]
    provider: [ "TGT_COMPANY_NAME_cleaned",      "TGT_COUNTRY" ]
"""
    path = _write_tmp(tmp_path, yml)
    rs: RuleSet = load_rules_from_path(path)

    # Tri par LevelN puis Fuzzy
    assert [r.level for r in rs.rules] == ["Level1", "Level5", "Fuzzy"]

    # Types de règles
    kinds = {r.level: r.kind for r in rs.rules}
    assert kinds["Level1"] == "exact"
    assert kinds["Level5"] == "exact"
    assert kinds["Fuzzy"] == "fuzzy"

    # Cardinalités cohérentes
    c1 = rs.by_level("Level1")
    assert c1 and c1.cardinality == 3
    c5 = rs.by_level("Level5")
    assert c5 and c5.cardinality == 2
    cf = rs.by_level("Fuzzy")
    assert cf and cf.cardinality == 2

def test_validation_errors(tmp_path):
    # mpm/provider longueurs différentes
    yml_bad = """
matching_criterias:
  LevelX:
    matching_rule: "Bad"
    mpm:      [ "A", "B" ]
    provider: [ "A" ]
"""
    p = _write_tmp(tmp_path, yml_bad)
    try:
        load_rules_from_path(p)
        assert False, "devrait lever une ValueError"
    except ValueError as e:
        assert "Longueurs différentes" in str(e)



from __future__ import annotations
from typing import Callable, Dict, List, Optional
import numpy as np
import polars as pl
from rapidfuzz import fuzz, process
from rapidfuzz.distance import Jaro, JaroWinkler

# ---- Registry des scorers (simples alias YAML -> fonctions) ----

def _scale01_to_100(fn):
    return lambda a, b: 100.0 * fn(a, b)

SCORER_REGISTRY: Dict[str, Callable[[str, str], float]] = {
    # Levenshtein-like
    "ratio": fuzz.ratio,
    "lev": fuzz.ratio,
    "partial_ratio": fuzz.partial_ratio,
    "partial": fuzz.partial_ratio,

    # Token-based
    "token_sort_ratio": fuzz.token_sort_ratio,
    "tsort": fuzz.token_sort_ratio,
    "token_set_ratio": fuzz.token_set_ratio,
    "tset": fuzz.token_set_ratio,

    # Heuristic combo
    "wratio": fuzz.WRatio,
    "WRatio": fuzz.WRatio,

    # Jaro / Jaro–Winkler (normalisés 0..100)
    "jaro": _scale01_to_100(Jaro().similarity),
    "jw": _scale01_to_100(JaroWinkler().similarity),
    "jaro_winkler": _scale01_to_100(JaroWinkler().similarity),
}


class RapidFuzzyMatcher:
    """
    Matching multi-colonnes avec RapidFuzz.
    - colonnes gauche via `columns`
    - colonnes droite via `peers` (mapping col_gauche -> col_droite). Si absent: on assume mêmes noms.
    - scorers par colonne (via SCORER_REGISTRY)
    - combinaison 'weighted' (moyenne pondérée) ou 'min' (maillon faible)
    - blocking optionnel (block_by_left / block_by_right)
    - seuils par colonne + seuil global
    - top_k candidats par left_id

    Sortie: DataFrame Polars avec:
      left_id, right_id, score, score_<col1>, score_<col2>, ...
    """

    def __init__(
        self,
        *,
        columns: List[str],
        scorers: Dict[str, Callable[[str, str], float]],
        weights: Optional[Dict[str, float]] = None,
        per_column_min: Optional[Dict[str, float]] = None,
        global_min: float = 90.0,
        combine_mode: str = "weighted",       # "weighted" | "min"
        block_by_left: Optional[str] = None,
        block_by_right: Optional[str] = None,
        top_k: int = 3,
        left_id: str = "id",
        right_id: str = "id",
        peers: Optional[Dict[str, str]] = None,
        normalize: bool = True,
    ):
        self.columns = columns
        self.scorers = scorers
        self.weights = weights or {c: 1.0 for c in columns}
        self.per_column_min = per_column_min or {}
        self.global_min = float(global_min)
        self.combine_mode = combine_mode
        self.block_by_left = block_by_left
        self.block_by_right = block_by_right or block_by_left
        self.top_k = int(top_k)
        self.left_id = left_id
        self.right_id = right_id
        self.peers = peers or {c: c for c in columns}
        self.normalize = bool(normalize)

        # Vérifications rapides
        missing_scorer = [c for c in self.columns if c not in self.scorers]
        if missing_scorer:
            raise ValueError(f"Scorer manquant pour colonnes: {missing_scorer}")
        unknown_weight = [c for c in self.weights if c not in self.columns]
        if unknown_weight:
            raise ValueError(f"Poids fourni pour colonnes inconnues: {unknown_weight}")
        unknown_peer = [c for c in self.columns if c not in self.peers]
        if unknown_peer:
            raise ValueError(f"Peer manquant pour colonnes: {unknown_peer}")
        if self.combine_mode not in ("weighted", "min"):
            raise ValueError("combine_mode doit être 'weighted' ou 'min'")

    # ---------- Normalisation légère ----------
    @staticmethod
    def _norm_str(s: Optional[str]) -> str:
        if s is None:
            return ""
        return str(s).strip().lower()

    def _prep_norm(self, df: pl.DataFrame, side: str) -> pl.DataFrame:
        if not self.normalize:
            return df
        exprs = []
        cols = self.columns if side == "left" else [self.peers[c] for c in self.columns if self.peers[c] in df.columns]
        for c in cols:
            if c in df.columns:
                exprs.append(pl.col(c).cast(pl.Utf8, strict=False).map_elements(self._norm_str))
        return df.with_columns(exprs) if exprs else df

    # ---------- Scoring d'un bloc ----------
    def _score_block(self, left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
        left_ids = left[self.left_id].to_list()
        right_ids = right[self.right_id].to_list()
        if not left_ids or not right_ids:
            return self._empty_out()

        # Matrices de similarité par colonne
        col_mats: Dict[str, np.ndarray] = {}
        for c in self.columns:
            lcol = c
            rcol = self.peers[c]
            if lcol not in left.columns or rcol not in right.columns:
                # colonne manquante => matrice de zéros
                col_mats[c] = np.zeros((len(left_ids), len(right_ids)), dtype=np.float64)
                continue
            lvals = ["" if v is None else v for v in left[lcol].to_list()]
            rvals = ["" if v is None else v for v in right[rcol].to_list()]
            mat = process.cdist(lvals, rvals, scorer=self.scorers[c], score_cutoff=0.0)
            col_mats[c] = mat.astype(np.float64, copy=False)

        # Combinaison
        if self.combine_mode == "weighted":
            total_w = float(sum(self.weights.get(c, 1.0) for c in self.columns)) or 1.0
            combined = sum(self.weights.get(c, 1.0) * col_mats[c] for c in self.columns) / total_w
        else:  # "min"
            mats = [col_mats[c] for c in self.columns]
            combined = np.minimum.reduce(mats)

        # Top-k par ligne gauche
        k = max(1, min(self.top_k, combined.shape[1]))
        idx_topk = np.argpartition(-combined, kth=k-1, axis=1)[:, :k]

        # Construction des lignes retenues
        rows = []
        for i, cols_j in enumerate(idx_topk):
            for j in cols_j:
                # Règle stricte par colonne
                pass_rule = True
                per_scores = {}
                for c in self.columns:
                    s = float(col_mats[c][i, j])
                    per_scores[c] = s
                    th = self.per_column_min.get(c, None)
                    if th is not None and s < float(th):
                        pass_rule = False

                wscore = float(combined[i, j])
                if pass_rule and wscore >= self.global_min:
                    row = {"left_id": left_ids[i], "right_id": right_ids[j], "score": wscore}
                    for c in self.columns:
                        row[f"score_{c}"] = per_scores[c]
                    rows.append(row)

        if not rows:
            return self._empty_out()
        out = pl.DataFrame(rows)
        return out.sort(by=["left_id", "score"], descending=[False, True])

    def _empty_out(self) -> pl.DataFrame:
        schema = {"left_id": pl.Utf8, "right_id": pl.Utf8, "score": pl.Float64}
        schema.update({f"score_{c}": pl.Float64 for c in self.columns})
        return pl.DataFrame(schema=schema)

    # ---------- API publique ----------
    def match(self, left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
        left_p = self._prep_norm(left, side="left")
        right_p = self._prep_norm(right, side="right")

        if self.block_by_left and self.block_by_right:
            parts = []
            right_groups = {k: g for k, g in right_p.group_by(self.block_by_right)}
            for k, g_left in left_p.group_by(self.block_by_left):
                g_right = right_groups.get(k)
                if g_right is None:
                    continue
                parts.append(self._score_block(g_left, g_right))
            if not parts:
                return self._empty_out()
            return pl.concat(parts, how="vertical").sort(by=["left_id", "score"], descending=[False, True])

        return self._score_block(left_p, right_p)




import polars as pl
from matching_engine.core.rapidfuzzy_matcher import RapidFuzzyMatcher, SCORER_REGISTRY

def test_weighted_jw_with_peers_and_blocking():
    left = pl.DataFrame({
        "LID": ["L1","L2","L3"],
        "name_left": ["BNP Paribas", "Societe Generale", "Credit Agricole"],
        "city_left": ["Paris","Marseille","Lyon"],
        "country_left": ["FR","FR","FR"],
    })
    right = pl.DataFrame({
        "RID": ["R1","R2","R3"],
        "name_right": ["Bnp Paribas SA", "Soc Gen", "Credit Agricole Group"],
        "city_right": ["Paris","Marseille","Lyon"],
        "country_right": ["FR","FR","FR"],
    })

    m = RapidFuzzyMatcher(
        columns=["name_left","city_left"],
        peers={"name_left":"name_right", "city_left":"city_right"},
        scorers={"name_left": SCORER_REGISTRY["jw"], "city_left": SCORER_REGISTRY["ratio"]},
        weights={"name_left": 0.8, "city_left": 0.2},
        per_column_min={"name_left": 88, "city_left": 95},
        global_min=92,
        combine_mode="weighted",
        block_by_left="country_left",
        block_by_right="country_right",
        top_k=3,
        left_id="LID",
        right_id="RID",
        normalize=True,
    )

    out = m.match(left, right)
    pairs = {(l, r) for l, r in zip(out["left_id"].to_list(), out["right_id"].to_list())}
    # On attend L1->R1 et L3->R3 (noms proches + ville = 100)
    assert ("L1","R1") in pairs
    assert ("L3","R3") in pairs
    # Soc Gen vs Société Générale: souvent < 88 en jw (selon normalisation), donc rejeté
    assert ("L2","R2") not in pairs
    assert "score_name_left" in out.columns and "score_city_left" in out.columns

def test_min_mode_strict_combination():
    left = pl.DataFrame({
        "id": ["L1"],
        "name": ["BNP Paribas"],
        "city": ["Paris"],
        "country": ["FR"],
    })
    right = pl.DataFrame({
        "id": ["R1","R2"],
        "name": ["Bnp Paribas SA", "BNP Paribas"],
        "city": ["Paris","Lyon"],
        "country": ["FR","FR"],
    })
    # Ici, mode "min": le score global devient le maillon faible (city)
    m = RapidFuzzyMatcher(
        columns=["name","city"],
        scorers={"name": SCORER_REGISTRY["jw"], "city": SCORER_REGISTRY["ratio"]},
        weights={"name": 1.0, "city": 1.0},        # ignoré en mode 'min'
        per_column_min={"name": 85, "city": 95},   # règle stricte
        global_min=90,
        combine_mode="min",
        block_by_left="country",
        block_by_right="country",
        top_k=2,
        left_id="id",
        right_id="id",
        normalize=True,
    )
    out = m.match(left, right)
    # R1: name ok, city Paris==Paris => passe; R2: city Lyon -> <95 => rejeté
    ids = out["right_id"].to_list()
    assert ids == ["R1"]



matching_criterias:

  Level1:
    matching_rule: "3WM Legal Name, Immat Country, LEI"
    mpm:      [ "LB_RAISON_SOCIAL_left_untouched", "CD_PAYS_IMMAT_left", "LEI_final" ]
    provider: [ "TGT_COMPANY_NAME_untouched",      "TGT_COUNTRY",        "LEI" ]

  Level2:
    matching_rule: "2WM Immat Country, LEI"
    mpm:      [ "CD_PAYS_IMMAT_left", "LEI_final" ]
    provider: [ "TGT_COUNTRY",        "LEI" ]

  Level3:
    matching_rule: "3WM Legal Name, Immat Country, ISIN"
    mpm:      [ "LB_RAISON_SOCIAL_left_original", "CD_PAYS_IMMAT_left", "ISIN_final" ]
    provider: [ "TGT_COMPANY_NAME_original",      "TGT_COUNTRY",        "ISIN" ]

  Level4:
    matching_rule: "2WM Immat Country, ISIN"
    mpm:      [ "CD_PAYS_IMMAT_left", "ISIN_final" ]
    provider: [ "TGT_COUNTRY",        "ISIN" ]

  Level5:
    matching_rule: "2WM Legal Name, Immat Country"
    mpm:      [ "LB_RAISON_SOCIAL_left_original", "CD_PAYS_IMMAT_left" ]
    provider: [ "TGT_COMPANY_NAME_original",      "TGT_COUNTRY" ]

  Level6:
    matching_rule: "2WM Legal Name (cleaned), Immat Country"
    mpm:      [ "LB_RAISON_SOCIAL_left_cleaned", "CD_PAYS_IMMAT_left" ]
    provider: [ "TGT_COMPANY_NAME_cleaned",      "TGT_COUNTRY" ]

  Level7:
    matching_rule: "1WM LEI"
    mpm:      [ "LEI_final" ]
    provider: [ "LEI" ]

  Level8:
    matching_rule: "1WM ISIN"
    mpm:      [ "ISIN_final" ]
    provider: [ "ISIN" ]

  Level9:
    matching_rule: "2WM Legal Name, Business Country"
    mpm:      [ "LB_RAISON_SOCIAL_left_original", "CD_PAYS_BUSINESS_left" ]
    provider: [ "TGT_COMPANY_NAME_original",      "TGT_COUNTRY" ]

  Level10:
    matching_rule: "2WM Legal Name (cleaned), Business Country"
    mpm:      [ "LB_RAISON_SOCIAL_left_cleaned", "CD_PAYS_BUSINESS_left" ]
    provider: [ "TGT_COMPANY_NAME_cleaned",      "TGT_COUNTRY" ]

  Fuzzy:
    matching_rule: "Fuzzy Legal Name, Immat Country"
    mpm:      [ "LB_RAISON_SOCIAL_left_cleaned", "CD_PAYS_IMMAT_left" ]
    provider: [ "TGT_COMPANY_NAME_cleaned",      "TGT_COUNTRY" ]
