# tests/test_exact_matcher_lazy.py
import polars as pl
from dataclasses import dataclass

# ⚠️ Adapte l'import ci-dessous à l'emplacement réel de ta classe
from src.core.exact_matcher_lazy import ExactMatcherLazy  # <-- modifie si besoin

@dataclass
class FakeRule:  # stub simple, suffit pour le test
    rmpm_cols: list[str]
    provider_cols: list[str]
    level: str
    name: str

def test_apply_rule_exact_join_returns_expected_row():
    # -- données LEFT (rmpm) --
    left_df = pl.DataFrame(
        {
            "ent_id_left": [1, 2],
            "name": ["ACME", "BETA"],
            "country": ["FR", "DE"],
        }
    ).lazy()

    # -- données RIGHT (provider) --
    right_df = pl.DataFrame(
        {
            "ent_id_right": ["A", "B"],
            "name": ["ACME", "GAMMA"],
            "country": ["FR", "ES"],
            "incorp_country": ["FR", "ES"],
        }
    ).lazy()

    # règle d’égalité sur (name, country)
    rule = FakeRule(
        rmpm_cols=["name", "country"],
        provider_cols=["name", "country"],
        level="L1",
        name="exact_name_country",
    )

    matcher = ExactMatcherLazy(left_id="ent_id_left", right_id="ent_id_right")

    out = matcher.apply_rule(left_df, right_df, rule).collect()

    # Vérifs : schéma, taille et valeurs
    assert out.columns == ["right_id", "left_id", "level", "rule_name", "country"]
    assert out.shape == (1, 5)

    row = out.row(0, named=True)
    assert row["right_id"] == "A"
    assert row["left_id"] == 1
    assert row["level"] == "L1"
    assert row["rule_name"] == "exact_name_country"
    assert row["country"] == "FR"
