import sys
import polars as pl
import pytest
from datetime import datetime, date

# ===== Imports du module réel (deux chemins possibles selon ton repo) =====
try:
    import src.core.fic as fic_mod               # <-- si ta classe est dans src/core/fic.py
    from src.core.fic import FICMatchingTable
except Exception:
    import src.fic as fic_mod                    # <-- sinon dans src/fic.py
    from src.fic import FICMatchingTable

# ===== Doubles légers =====
class DummyDb:
    """Double de DbClient : enregistre les appels et renvoie des DataFrames."""
    def __init__(self):
        self.read_calls = []
        self.insert_calls = []

    def read_as_polars(self, query, schema=None, params=None):
        self.read_calls.append({"query": query, "schema": schema, "params": params})
        # DataFrame vide mais avec schéma si fourni
        if isinstance(schema, dict) and schema:
            return pl.DataFrame({k: pl.Series([], dtype=v) for k, v in schema.items()})
        return pl.DataFrame()

    def insert_records(self, query, values):
        self.insert_calls.append({"query": query, "values": values})

class DummyMatchingTable:
    pass


# ===== Fixtures =====
@pytest.fixture
def db():
    return DummyDb()

@pytest.fixture
def table(db):
    # t_fic_cond est ce que ton code formate avec .format(provider=...)
    return FICMatchingTable(
        db=db,
        mtbl=DummyMatchingTable(),
        t_fic_cond="provider = '{provider}'",
        t_fic_ref="t_ref_fic_client",
    )


# ===== Tests =====

def test_load_fic_data_query_and_schema(table, db):
    lf = table.load_fic_data("BNP")
    assert isinstance(lf, pl.LazyFrame)

    call = db.read_calls[-1]
    assert "FROM t_ref_fic_client" in call["query"]
    assert "provider = 'BNP'" in call["query"]                      # substitution du provider
    assert set(call["schema"].keys()) == {"intern_id", "target_id", "target_name"}


def test_compare_dataframes_lazy_detects_new_and_to_close(table):
    # existing (ouvert) = 3 lignes dont 1 avec matching_rule='FIC'
    df_exist = pl.DataFrame(
        [
            {"intern_id": "iA", "target_id": "tX", "target_name": "P1",
             "cluster_id": "c1", "matching_end_date": None, "matching_rule": "RULE"},
            {"intern_id": "iB", "target_id": "tB", "target_name": "P1",
             "cluster_id": "c2", "matching_end_date": None, "matching_rule": "RULE"},
            {"intern_id": "iC", "target_id": "tC", "target_name": "P1",
             "cluster_id": "c3", "matching_end_date": None, "matching_rule": "FIC"},
        ],
        schema={"intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8,
                "cluster_id": pl.Utf8, "matching_end_date": pl.Date, "matching_rule": pl.Utf8},
    )

    # input = A (match sur intern_id), B' (match sur target_id), D (nouveau)
    df_in = pl.DataFrame(
        [
            {"intern_id": "iA", "target_id": "tNEW", "target_name": "P1"},
            {"intern_id": "iNEW", "target_id": "tB",   "target_name": "P1"},
            {"intern_id": "iD", "target_id": "tD", "target_name": "P1"},
        ],
        schema={"intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8},
    )

    new_entries_lf, to_close_lf = table._compare_dataframes_lazy(df_exist.lazy(), df_in.lazy())
    new_entries = new_entries_lf.collect()
    to_close = to_close_lf.collect()

    # D doit être nouveau
    assert {"intern_id": "iD", "target_id": "tD", "target_name": "P1"} in \
           new_entries.select(["intern_id", "target_id", "target_name"]).to_dicts()

    # C (FIC) doit être à clôturer car absent de l'input
    assert {"intern_id": "iC", "target_id": "tC", "target_name": "P1"} in \
           to_close.select(["intern_id", "target_id", "target_name"]).to_dicts()

    # colonnes ajoutées par la méthode
    assert {"matching_end_date", "closure_reason"} <= set(to_close.columns)
    assert "matching_start_date" in new_entries.columns


def test_close_old_records_emits_update(table, db):
    to_close = pl.DataFrame(
        [
            {"intern_id": "i1", "target_id": "t1", "target_name": "P",
             "matching_end_date": date(2024, 1, 1), "closure_reason": "NO_LONGER_EXISTS"},
            {"intern_id": "i2", "target_id": "t2", "target_name": "P",
             "matching_end_date": date(2024, 1, 2), "closure_reason": "NO_LONGER_EXISTS"},
        ]
    )
    table._close_old_records(to_close)

    assert len(db.insert_calls) == 1
    call = db.insert_calls[0]
    assert "UPDATE" in call["query"]
    assert len(call["values"]) == 2
    # ordre (end_date, reason, intern_id, target_id, target_name)
    assert call["values"][0][2] == "i1" and call["values"][0][3] == "t1"


def test_insert_new_records_emits_insert(table, db):
    new_entries = pl.DataFrame(
        [
            {"intern_id": "i1", "target_id": "t1", "target_name": "P", "cluster_id": "c1"},
            {"intern_id": "i2", "target_id": "t2", "target_name": "P", "cluster_id": "c2"},
        ]
    )

    table._insert_new_records(new_entries)

    assert len(db.insert_calls) == 1
    call = db.insert_calls[0]
    assert "INSERT INTO" in call["query"]
    assert len(call["values"]) == 2
    # valeurs statiques selon ton implémentation
    assert call["values"][0][3] == "RMPH"        # intern_name
    assert call["values"][0][4] == "FIC"         # matching_rule
    assert call["values"][0][6] is False         # is_duplicate
    assert call["values"][0][7] == "c1"          # cluster_id


def test_manage_fic_pipeline(monkeypatch, table, db):
    """Orchestration complète + vérifie recalculate_clusters(fic=False)."""

    # 1) monkeypatch load_fic_data -> un input simple
    input_df = pl.DataFrame(
        [{"intern_id": "i1", "target_id": "t1", "target_name": "P"}],
        schema={"intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8},
    )
    monkeypatch.setattr(table, "load_fic_data", lambda provider_list: input_df.lazy())

    # 2) monkeypatch _load_existing_fic_records -> entrée différente pour forcer un insert
    exist_df = pl.DataFrame(
        [{"intern_id": "iX", "target_id": "tX", "target_name": "P", "cluster_id": "cX",
          "matching_end_date": None, "matching_rule": "FIC"}],
        schema={"intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8,
                "cluster_id": pl.Utf8, "matching_end_date": pl.Date, "matching_rule": pl.Utf8},
    )
    monkeypatch.setattr(table, "_load_existing_fic_records", lambda *a, **k: exist_df.lazy())

    # 3) monkeypatch recalculate_clusters DANS LE MODULE fic (là où c’est importé)
    flags = {"called": False, "fic": None}
    def fake_recalc(df, fic=False):
        flags["called"] = True
        flags["fic"] = fic
        return df.with_columns(pl.lit("cNEW").alias("cluster_id"))

    # Patch dans l'espace de noms du module où la classe est définie
    monkeypatch.setattr(fic_mod, "recalculate_clusters", fake_recalc, raising=False)

    # 4) monkeypatch load_fic_after_update -> résultat final simulé
    final_df = pl.DataFrame(
        [{"intern_id": "i1", "target_id": "t1", "target_name": "P",
          "cluster_id": "cNEW", "matching_end_date": None, "matching_rule": "FIC"}]
    )
    monkeypatch.setattr(table, "load_fic_after_update", lambda provider: final_df)

    out = table.manage_fic(provider_list=["P"])

    assert isinstance(out, pl.DataFrame)
    assert flags["called"] is True and flags["fic"] is False   # important : fic=False
    assert any("INSERT INTO" in c["query"] for c in db.insert_calls)
