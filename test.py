# test_run_provider.py
import types
import logging
import builtins
import importlib
import pytest

MODULE_NAME = "run_provider"


class _FakeDataCfg:
    fic_base_condition = "base_cond"
    fic_table = "fic_table"
    country_scope = ["FR", "DE"]


class _FakeLoaded:
    def __init__(self):
        self.ruleset = {"rules": ["r1", "r2"]}
        self.left_id = "left_ent_id"
        self.right_id = "right_ent_id"
        self.left_lf = "LEFT_FRAME"
        self.right_lf = "RIGHT_FRAME"


class Calls:
    def __init__(self):
        self.rules_for_provider = []
        self.load_data = 0
        self.unified_loader_init = []
        self.unified_loader_load = []
        self.pipeline_init = []
        self.pipeline_run = []
        self.apply_prefilter = []
        self.process_matching = []


@pytest.fixture
def calls():
    return Calls()


@pytest.fixture(autouse=True)
def patch_module(monkeypatch, calls):
    """
    On importe le module cible puis on remplace TOUTES ses dépendances par des fakes.
    """
    mod = importlib.import_module(MODULE_NAME)

    # Fake: load_rules_for_provider
    def _rules(provider):
        calls.rules_for_provider.append(provider)
        return {"rules": ["r1"]}

    # Fake: load_data_io_config
    def _load_cfg(_):
        calls.load_data += 1
        return _FakeDataCfg()

    # Fake: MatchingTable
    class _MT:
        def __init__(self, db):
            self.db = db

        def process_matching(self, provider, matching_output, country):
            calls.process_matching.append((provider, matching_output, country))

    # Fake: FICMatchingTable
    class _FIC:
        def __init__(self, db, mtbl, base_cond, table_name):
            self.db = db
            self.mtbl = mtbl
            self.base_cond = base_cond
            self.table_name = table_name

        def manage_fic(self, provider):
            return {("A", "B"), ("C", "D")}  # paires fictives

    # Fake: UnifiedLazyLoader
    class _Loader:
        def __init__(self, db, data_cfg, rule_set):
            calls.unified_loader_init.append((db, data_cfg, rule_set))

        def load_for_provider(self, provider, country_condition, include_fuzzy):
            calls.unified_loader_load.append(
                (provider, country_condition, include_fuzzy)
            )
            return _FakeLoaded()

    # Fake: MatchingPipelineLazy
    class _Pipeline:
        def __init__(self, ruleset, left_id, right_id):
            calls.pipeline_init.append((ruleset, left_id, right_id))

        def run(self, left_lf, right_lf, left_id, right_id, fic_pairs):
            result = [("L1", "R1"), ("L2", "R2")]
            calls.pipeline_run.append(
                (left_lf, right_lf, left_id, right_id, fic_pairs, tuple(result))
            )
            return result

    # Fake: apply_fic_prefilter
    def _apply(left_lf, fic_pairs, left_id, right_id, run_fuzzy):
        calls.apply_prefilter.append(
            (left_lf, fic_pairs, left_id, right_id, run_fuzzy)
        )
        return f"FILTERED({left_lf})"

    monkeypatch.setattr(mod, "load_rules_for_provider", _rules)
    monkeypatch.setattr(mod, "load_data_io_config", _load_cfg)
    monkeypatch.setattr(mod, "MatchingTable", _MT)
    monkeypatch.setattr(mod, "FICMatchingTable", _FIC)
    monkeypatch.setattr(mod, "UnifiedLazyLoader", _Loader)
    monkeypatch.setattr(mod, "MatchingPipelineLazy", _Pipeline)
    monkeypatch.setattr(mod, "apply_fic_prefilter", _apply)

    yield
    # (rien à restaurer manuellement, monkeypatch gère la remise à zéro)


def test_run_happy_path(calls, caplog):
    import run_provider as mod

    caplog.set_level(logging.INFO)
    runner = mod.ProviderRunner(db=object())

    runner.run("ACME")

    # Règles et config bien chargées
    assert calls.rules_for_provider == ["ACME"]
    assert calls.load_data == 1

    # Loader et pipeline initialisés 2x (FR, DE)
    assert len(calls.unified_loader_init) == 1  # une seule fois, en dehors de la boucle
    assert calls.unified_loader_load == [
        ("ACME", "FR", True),
        ("ACME", "DE", True),
    ]
    assert len(calls.pipeline_init) == 2

    # Prefilter + run appelés 2x
    assert len(calls.apply_prefilter) == 2
    assert len(calls.pipeline_run) == 2

    # Insertion appelée avec les bons pays
    assert calls.process_matching == [
        ("ACME", [("L1", "R1"), ("L2", "R2")], "FR"),
        ("ACME", [("L1", "R1"), ("L2", "R2")], "DE"),
    ]

    # Logs attendus
    assert "Running matching for provider ACME" in caplog.text
    assert "Matching done for scope FR" in caplog.text
    assert "Matching done for scope DE" in caplog.text
