import types
import polars as pl
import pytest

# ---------- Doubles / Mocks légers ----------

class DummyDb:
    """Mock minimal de DbClient pour intercepter les appels."""
    def __init__(self):
        self.read_calls = []
        self.insert_calls = []

    def read_as_polars(self, query, schema=None, params=None):
        # On enregistre l'appel et, si un DataFrame a été programmé pour ce call_id, on le renvoie
        self.read_calls.append({"query": query, "schema": schema, "params": params})
        # Par défaut, renvoie un DataFrame vide avec le schéma demandé
        if isinstance(schema, dict) and schema:
            return pl.DataFrame({k: pl.Series([], dtype=v) for k, v in schema.items()})
        return pl.DataFrame()

    def insert_records(self, query, values):
        self.insert_calls.append({"query": query, "values": values})


# On importe le module testé : on colle ici une petite façade
# pour éviter la dépendance au package complet du projet.
# ==> On recolle juste la classe telle qu'extraite.
from datetime import datetime
def recalculate_clusters(df: pl.DataFrame, fic: bool = False) -> pl.DataFrame:  # sera monkeypatché dans les tests
    return df

class MatchingTable:  # placeholder
    pass

class FICMatchingTable:
    def __init__(self, db: DummyDb, mtbl: MatchingTable, t_fic_cond: str, t_fic_ref: str = "t_ref_fic_client"):
        self._t_match: str = "own_87645.tpn_t_matching_entity"
        self._db = db
        self._mtbl = mtbl
        self._t_fic_ref = t_fic_ref
        self._t_match = t_fic_cond            # (note: tel que dans la capture)
        self._fic_condition = t_fic_cond

    def load_fic_data(self, provider: str) -> pl.LazyFrame:
        q = f"""
        SELECT
            corr_id::text AS intern_id,
            tgt_id::text AS target_id,
            provider::text AS target_name
        FROM {self._t_fic_ref}
        WHERE {self._fic_condition.format(provider=provider)}
        """
        schema = {"intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8}
        df = self._db.read_as_polars(q, schema)
        return df.lazy()

    def _load_existing_fic_records(self, providers: list, rmpm_ids: list, tgt_ids: list) -> pl.LazyFrame:
        query = f"""
        SELECT intern_id, target_id, target_name, cluster_id, matching_end_date, matching_rule
        FROM own_87645.tpn_t_matching_entity
        WHERE target_name IN ({','.join(['%s'] * len(providers))})
        AND (intern_id IN ({','.join(['%s'] * len(rmpm_ids))})
        OR target_id IN ({','.join(['%s'] * len(tgt_ids))})
        OR matching_rule='FIC')
        AND matching_end_date IS NULL
        """
        schema = {
            "intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8,
            "cluster_id": pl.Utf8, "matching_end_date": pl.Date, "matching_rule": pl.Utf8,
        }
        df = self._db.read_as_polars(query, schema, params=tuple(providers)+tuple(rmpm_ids)+tuple(tgt_ids))
        return df.lazy()

    def _compare_dataframes_lazy(self, lf_existing: pl.LazyFrame, lf_input: pl.LazyFrame):
        existing_cols = lf_existing.columns
        matched_intern = lf_existing.join(lf_input, on=["intern_id", "target_name"], how="inner").select(existing_cols)
        matched_target = lf_existing.join(lf_input, on=["target_id", "target_name"], how="inner").select(existing_cols)
        fic = lf_existing.filter(pl.col("matching_rule") == "FIC")
        combined = pl.concat([matched_intern, matched_target, fic], how="vertical_relaxed").unique(subset=existing_cols)

        key_cols = ["intern_id", "target_id", "target_name"]
        new_entries = (
            lf_input.join(combined, on=key_cols, how="anti")
            .with_columns(pl.lit(datetime.now()).alias("matching_start_date"))
        )
        to_close = (
            combined.join(lf_input, on=key_cols, how="anti")
            .with_columns(
                pl.lit(datetime.now()).alias("matching_end_date"),
                pl.lit("NO_LONGER_EXISTS").alias("closure_reason"),
            )
        )
        return new_entries, to_close

    def _close_old_records(self, to_close: pl.DataFrame) -> None:
        update_query = f"""
        UPDATE {self._t_match}
        SET matching_end_date = %s, closure_reason = %s
        WHERE intern_id = %s AND target_id = %s AND target_name = %s
        """
        values = [
            (row["matching_end_date"], row["closure_reason"], row["intern_id"], row["target_id"], row["target_name"])
            for row in to_close.iter_rows(named=True)
        ]
        self._db.insert_records(update_query, values)

    def _insert_new_records(self, new_entries: pl.DataFrame) -> None:
        insert_query = f"""
        INSERT INTO {self._t_match} (
            intern_id, target_id, target_name, intern_name, matching_rule,
            matching_start_date, matching_end_date, closure_reason, is_duplicate, cluster_id
        )
        VALUES (%s, %s, %s, %s, %s, %s, NULL, NULL, %s, %s)
        ON CONFLICT DO NOTHING
        """
        values = [
            (
                row["intern_id"], row["target_id"], row["target_name"],
                "RMPH", "FIC", datetime.now(), False, row["cluster_id"]
            )
            for row in new_entries.iter_rows(named=True)
        ]
        self._db.insert_records(insert_query, values)

    def load_fic_after_update(self, provider):
        query = f"""
        SELECT intern_id, target_id, target_name, cluster_id, matching_end_date, matching_rule
        FROM {self._t_match}
        WHERE target_name='{provider}'
        AND matching_rule='FIC'
        AND matching_end_date IS NULL
        """
        schema = {
            "intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8,
            "cluster_id": pl.Utf8, "matching_end_date": pl.Date, "matching_rule": pl.Utf8,
        }
        return self._db.read_as_polars(query, schema=schema)

    def manage_fic(self, provider_list) -> pl.DataFrame:
        df_input = self.load_fic_data(provider_list)

        df_existing = self._load_existing_fic_records(
            df_input.select(pl.col("target_name")).unique().collect().to_series().to_list(),
            df_input.select(pl.col("intern_id")).unique().collect().to_series().to_list(),
            df_input.select(pl.col("tgt_id")).unique().collect().to_series().to_list(),
        )

        new_entries, to_close = self._compare_dataframes_lazy(df_existing, df_input)

        if not to_close.collect().is_empty():
            self._close_old_records(to_close.collect())

        if not new_entries.collect().is_empty():
            ne = recalculate_clusters(new_entries.collect(), fic=False)
            self._insert_new_records(ne)

        return self.load_fic_after_update(provider=provider_list)


# ---------- Helpers ----------

def df_lf(rows, schema):
    """Crée un DataFrame/LazyFrame Polars à partir d'une liste de dicts."""
    df = pl.DataFrame(rows, schema=schema)
    return df, df.lazy()


# ---------- Tests ----------

def test_load_fic_data_calls_db_and_returns_lazy():
    db = DummyDb()
    mt = MatchingTable()
    table = FICMatchingTable(db, mt, t_fic_cond="provider = '{provider}'", t_fic_ref="t_ref_fic_client")

    lf = table.load_fic_data("BNP")
    assert isinstance(lf, pl.LazyFrame)

    # Le dernier appel de read_as_polars doit contenir la clause provider formatée
    assert "provider = 'BNP'" in db.read_calls[-1]["query"]
    # Le schéma demandé doit être celui attendu
    assert set(db.read_calls[-1]["schema"].keys()) == {"intern_id", "target_id", "target_name"}


def test_compare_dataframes_lazy_new_and_to_close():
    # existing : A (intern match), B (target match), C (FIC seule)
    existing_rows = [
        {"intern_id": "iA", "target_id": "tX", "target_name": "P1", "cluster_id": "c1", "matching_end_date": None, "matching_rule": "RULE"},
        {"intern_id": "iB", "target_id": "tB", "target_name": "P1", "cluster_id": "c2", "matching_end_date": None, "matching_rule": "RULE"},
        {"intern_id": "iC", "target_id": "tC", "target_name": "P1", "cluster_id": "c3", "matching_end_date": None, "matching_rule": "FIC"},
    ]
    schema_exist = {
        "intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8,
        "cluster_id": pl.Utf8, "matching_end_date": pl.Date, "matching_rule": pl.Utf8,
    }
    df_exist, lf_exist = df_lf(existing_rows, schema_exist)

    # input : A (même intern_id + target_name), B' (même target_id + target_name), D (nouveau)
    input_rows = [
        {"intern_id": "iA", "target_id": "tNEW", "target_name": "P1"},
        {"intern_id": "iNEW", "target_id": "tB",   "target_name": "P1"},
        {"intern_id": "iD", "target_id": "tD", "target_name": "P1"},
    ]
    schema_input = {"intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8}
    df_in, lf_in = df_lf(input_rows, schema_input)

    table = FICMatchingTable(DummyDb(), MatchingTable(), "provider='{provider}'")

    new_entries_lf, to_close_lf = table._compare_dataframes_lazy(lf_exist, lf_in)

    new_entries = new_entries_lf.collect()
    to_close = to_close_lf.collect()

    # D est nouveau
    assert {"intern_id": "iD", "target_id": "tD", "target_name": "P1"} in new_entries.select(["intern_id","target_id","target_name"]).to_dicts()

    # C (FIC) doit être clôturé si absent de l'input
    assert {"intern_id": "iC", "target_id": "tC", "target_name": "P1"} in to_close.select(["intern_id","target_id","target_name"]).to_dicts()

    # Les colonnes de fin/raison existent dans to_close
    assert "matching_end_date" in to_close.columns
    assert "closure_reason" in to_close.columns


def test_close_old_records_calls_db_insert_records():
    db = DummyDb()
    table = FICMatchingTable(db, MatchingTable(), "provider='{provider}'")

    to_close = pl.DataFrame([
        {"intern_id": "i1", "target_id": "t1", "target_name": "P", "matching_end_date": pl.date(2024,1,1), "closure_reason": "NO_LONGER_EXISTS"},
        {"intern_id": "i2", "target_id": "t2", "target_name": "P", "matching_end_date": pl.date(2024,1,2), "closure_reason": "NO_LONGER_EXISTS"},
    ])

    table._close_old_records(to_close)

    assert len(db.insert_calls) == 1
    call = db.insert_calls[0]
    assert "UPDATE" in call["query"]
    # 2 lignes envoyées
    assert len(call["values"]) == 2
    # ordre des champs respecté
    assert call["values"][0][2] == "i1" and call["values"][0][3] == "t1"


def test_insert_new_records_calls_db_insert_records(monkeypatch):
    db = DummyDb()
    table = FICMatchingTable(db, MatchingTable(), "provider='{provider}'")

    new_entries = pl.DataFrame([
        {"intern_id": "i1", "target_id": "t1", "target_name": "P", "cluster_id": "c1"},
        {"intern_id": "i2", "target_id": "t2", "target_name": "P", "cluster_id": "c2"},
    ])

    table._insert_new_records(new_entries)

    assert len(db.insert_calls) == 1
    call = db.insert_calls[0]
    assert "INSERT INTO" in call["query"]
    assert len(call["values"]) == 2
    # Champs statiques : intern_name, matching_rule, is_duplicate
    # (on ne vérifie pas le datetime exact)
    assert call["values"][0][3] == "RMPH"
    assert call["values"][0][4] == "FIC"
    assert call["values"][0][6] is False
    assert call["values"][0][7] == "c1"


def test_manage_fic_end_to_end(monkeypatch):
    """Vérifie l'orchestration : load -> compare -> close/insert -> read final + recalculate_clusters fic=False."""
    db = DummyDb()
    table = FICMatchingTable(db, MatchingTable(), "provider='{provider}'")

    # 1) Mock load_fic_data (input)
    input_df = pl.DataFrame(
        [{"intern_id": "i1", "target_id": "t1", "target_name": "P"}],
        schema={"intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8},
    )
    monkeypatch.setattr(table, "load_fic_data", lambda provider_list: input_df.lazy())

    # 2) Mock _load_existing_fic_records (existing) -> différent pour forcer un insert
    exist_df = pl.DataFrame(
        [{"intern_id": "iX", "target_id": "tX", "target_name": "P", "cluster_id": "cX",
          "matching_end_date": None, "matching_rule": "FIC"}],
        schema={"intern_id": pl.Utf8, "target_id": pl.Utf8, "target_name": pl.Utf8,
                "cluster_id": pl.Utf8, "matching_end_date": pl.Date, "matching_rule": pl.Utf8},
    )
    monkeypatch.setattr(table, "_load_existing_fic_records", lambda *a, **k: exist_df.lazy())

    # 3) Mock recalculate_clusters -> ajoute cluster_id pour l'insert
    called = {"flag": False, "fic_arg": None}
    def fake_recalc(df, fic=False):
        called["flag"] = True
        called["fic_arg"] = fic
        return df.with_columns(pl.lit("cNEW").alias("cluster_id"))
    monkeypatch.setattr(__import__(__name__), "recalculate_clusters", fake_recalc)

    # 4) Mock load_fic_after_update pour renvoyer un résultat final
    final_df = pl.DataFrame(
        [{"intern_id": "i1", "target_id": "t1", "target_name": "P", "cluster_id": "cNEW",
          "matching_end_date": None, "matching_rule": "FIC"}]
    )
    monkeypatch.setattr(table, "load_fic_after_update", lambda provider: final_df)

    out = table.manage_fic(provider_list=["P"])
    # a) on a bien un DataFrame final
    assert isinstance(out, pl.DataFrame)
    # b) recalculate_clusters appelé avec fic=False
    assert called["flag"] is True and called["fic_arg"] is False
    # c) un insert a été émis
    assert any("INSERT INTO" in c["query"] for c in db.insert_calls)
