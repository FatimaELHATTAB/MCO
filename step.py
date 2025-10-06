# tests/test_matching_insertion.py
import types
from datetime import datetime, timedelta

import polars as pl
import pytest

# On importe la classe depuis ton module
# adapte l'import si le nom du fichier/module diffère
from matching_insertion import MatchingTable


# ---------- Helpers / Fixtures ----------

class FakeDB:
    """DB client minimaliste pour capturer les requêtes et simuler les retours."""
    def __init__(self, read_df: pl.DataFrame | None = None):
        self.read_df = read_df if read_df is not None else pl.DataFrame()
        self.last_query = None
        self.last_params = None
        self.insert_calls = []  # liste des (query, params)

    def read_as_polars(self, query: str, schema=None):
        self.last_query = query
        # on ignore 'schema' volontairement
        return self.read_df

    def insert_records(self, query: str, params):
        self.last_query = query
        self.last_params = params
        self.insert_calls.append((query, list(params)))


@pytest.fixture
def monkeypatch_schema(monkeypatch):
    # get_matching_table_schema n'est pas utile au test -> on le neutralise
    import matching_insertion as mi
    monkeypatch.setattr(mi, "get_matching_table_schema", lambda: None)
    return monkeypatch


@pytest.fixture
def monkeypatch_recalc_identity(monkeypatch):
    # recalculate_clusters : identité pour simplifier
    import matching_insertion as mi
    monkeypatch.setattr(mi, "recalculate_clusters", lambda df: df)
    return monkeypatch


# ---------- Tests load_provider_data ----------

def test_load_provider_data_builds_expected_query(monkeypatch_schema):
    df = pl.DataFrame({"intern_id": [1]})
    fdb = FakeDB(read_df=df)
    mt = MatchingTable(fdb, t_match="table_x")

    out = mt.load_provider_data(provider_id="ProviderX")

    assert out.shape == (1, 1)
    q = fdb.last_query
    assert "FROM table_x" in q
    assert "ProviderX" in q
    assert "matching_end_date IS NULL" in q
    assert "closure_reason IS NULL" in q
    assert "matching_rule != 'FIC'" in q


# ---------- Tests format_matching_output ----------

def test_format_matching_output_on_empty_returns_same():
    fdb = FakeDB()
    mt = MatchingTable(fdb)

    empty = pl.DataFrame()
    out = mt.format_matching_output(empty, provider="FR")

    assert out.frame_equal(empty)


def test_format_matching_output_maps_and_adds_columns():
    fdb = FakeDB()
    mt = MatchingTable(fdb)

    raw = pl.DataFrame(
        {
            "right_id": [101],
            "left_id": [202],
            "intern_name": ["Alice"],
            "target_name": ["ACME"],
        }
    )

    out = mt.format_matching_output(raw, provider="FR")

    expected_cols = {
        "intern_id",
        "target_id",
        "intern_name",
        "target_name",
        "matching_rule",
        "matching_start_date",
        "matching_end_date",
        "closure_reason",
        "is_duplicate",
        "cluster_id",
        "lisbon_decision",
        "lisbon_date",
        "lisbon_user",
        "confidence_score",
        "country",
    }
    assert set(out.columns) == expected_cols
    row = out.row(0, named=True)
    assert row["intern_id"] == 101
    assert row["target_id"] == 202
    assert row["country"] == "FR"


# ---------- Tests generate_db_entries (logique de diff) ----------

def test_generate_db_entries_labels_cover_all_cases():
    mt = MatchingTable(FakeDB())

    # Données actuelles (en base)
    current = pl.DataFrame(
        {
            "intern_id": [1, 2, 3],
            "target_id": ["A", "B", "C"],
            "matching_rule": ["R", "R", "R"],
            "cluster_id": ["C1", "C1", "C1"],
        }
    )

    # Nouvelles données:
    # - intern 1 : target change A -> A2  => "Target Changed"
    # - intern 2 : cluster change C1 -> C2 => "Cluster Changed"
    # - intern 3 : disparaît                 => "Target Removed"
    new = pl.DataFrame(
        {
            "intern_id": [1, 2],
            "target_id": ["A2", "B"],
            "matching_rule": ["R", "R"],
            "cluster_id": ["C1", "C2"],
        }
    )

    new_entries, to_close = mt.generate_db_entries(current, new)

    # new_entries : les lignes qui n'existaient pas (anti-join complet)
    # Ici : (1, A2, R, C1) et (2, B, R, C2)
    assert new_entries.shape[0] == 2

    # to_close doit contenir 3 lignes à fermer (les anciennes versions)
    assert to_close.shape[0] == 3
    # On vérifie la présence des trois motifs de fermeture
    reasons = set(to_close["closure_reason"].to_list())
    assert "Target Removed" in reasons
    assert "Target Changed" in reasons or "Rule Changed" in reasons or "Cluster Changed" in reasons
    assert "Cluster Changed" in reasons


# ---------- Tests write_updated_data / _insert_new_data ----------

def test_write_updated_data_no_updates_logs_and_returns(caplog):
    fdb = FakeDB()
    mt = MatchingTable(fdb)

    mt.write_updated_data("ProviderX", pl.DataFrame())

    # aucune insertion
    assert fdb.insert_calls == []
    # message d'info
    assert any("No updates to write" in rec.message for rec in caplog.records)


def test_write_updated_data_calls_private_insert(monkeypatch):
    fdb = FakeDB()
    mt = MatchingTable(fdb)

    called = {"ok": False}

    def _fake_insert(rows, batch_size):
        called["ok"] = True
        assert isinstance(rows, pl.DataFrame)
        assert batch_size == 1000

    monkeypatch.setattr(mt, "_insert_new_data", _fake_insert)

    rows = pl.DataFrame(
        {
            "intern_id": [1],
            "target_id": [2],
            "intern_name": ["A"],
            "target_name": ["B"],
            "matching_rule": ["R"],
            "matching_start_date": [datetime.utcnow()],
            "matching_end_date": [None],
            "closure_reason": [None],
            "lisbon_decision": [None],
            "lisbon_date": [None],
            "lisbon_user": [None],
            "cluster_id": ["C1"],
            "confidence_score": [None],
            "is_duplicate": [False],
            "country": ["FR"],
        }
    )

    mt.write_updated_data("ProviderX", rows)
    assert called["ok"] is True


def test__insert_new_data_batches_and_calls_db():
    # 3 lignes, batch_size=2 => 2 appels (2 + 1)
    rows = pl.DataFrame(
        {
            "intern_id": [1, 2, 3],
            "target_id": [11, 22, 33],
            "intern_name": ["a", "b", "c"],
            "target_name": ["x", "y", "z"],
            "matching_rule": ["R", "R", "R"],
            "matching_start_date": [datetime.utcnow()] * 3,
            "matching_end_date": [None, None, None],
            "closure_reason": [None, None, None],
            "lisbon_decision": [None, None, None],
            "lisbon_date": [None, None, None],
            "lisbon_user": [None, None, None],
            "cluster_id": ["C1", "C1", "C1"],
            "confidence_score": [None, None, None],
            "is_duplicate": [False, False, False],
            "country": ["FR", "FR", "FR"],
        }
    )
    fdb = FakeDB()
    mt = MatchingTable(fdb)

    mt._insert_new_data(rows_to_update=rows, batch_size=2)

    assert len(fdb.insert_calls) == 2
    # tailles de batch : 2 puis 1
    assert len(fdb.insert_calls[0][1]) == 2
    assert len(fdb.insert_calls[1][1]) == 1


# ---------- Tests update_closed_entries ----------

def test_update_closed_entries_empty_noop():
    fdb = FakeDB()
    mt = MatchingTable(fdb)

    mt.update_closed_entries(pl.DataFrame(), provider="ACME")

    assert fdb.insert_calls == []


def test_update_closed_entries_ok():
    fdb = FakeDB()
    mt = MatchingTable(fdb)

    df_to_close = pl.DataFrame(
        {
            "intern_id": [1, 2],
            "closure_reason": ["Target Removed", "Cluster Changed"],
            "matching_end_date": [datetime.utcnow(), datetime.utcnow() + timedelta(seconds=1)],
        }
    )

    mt.update_closed_entries(df_to_close, provider="ACME", batch_size=10)

    assert len(fdb.insert_calls) == 1
    query, params = fdb.insert_calls[0]
    assert "UPDATE" in query and "ACME" in query
    # deux lignes envoyées
    assert len(params) == 2
