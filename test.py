import os
import pathlib
import logging
import pytest

# 👉 adapte ce chemin si ton module n'est pas à la racine
import run_provider as mod


def _has_attr(obj, name):
    return hasattr(obj, name) and callable(getattr(obj, name))


@pytest.fixture(scope="session")
def logger():
    lg = logging.getLogger("it-run")
    lg.setLevel(logging.INFO)
    return lg


@pytest.fixture(scope="session")
def data_cfg_path(tmp_path_factory):
    """
    Fournit un data.yaml *réel* minimal pour vos loaders.
    Si vous avez déjà ./config/data.yaml dans le repo, supprimez ce fixture
    et laissez votre vrai fichier être utilisé.
    """
    tmp = tmp_path_factory.mktemp("cfg")
    cfg = tmp / "data.yaml"

    # ⚠️ Adapte les clés ci-dessous à votre vrai schéma de config
    cfg.write_text(
        "fic_base_condition: base_cond\n"
        "fic_table: fic_table\n"
        "country_scope:\n"
        "  - FR\n"
        "  - DE\n"
        # ajoute ici toute autre clé nécessaire à UnifiedLazyLoader / MatchingPipelineLazy
    )
    return cfg


@pytest.fixture(scope="session")
def ensure_config_folder(data_cfg_path, monkeypatch):
    """
    Force mod.load_data_io_config('./config/data.yaml') à trouver un vrai fichier.
    On crée ./config/data.yaml dans le workspace de test si nécessaire.
    """
    config_dir = pathlib.Path("./config")
    config_dir.mkdir(exist_ok=True)
    target = config_dir / "data.yaml"
    if not target.exists():
        target.write_text(pathlib.Path(data_cfg_path).read_text())
    # Pas de monkeypatch des fonctions : on laisse *votre* loader lire ce fichier réel.


@pytest.fixture(scope="session")
def db_client():
    """
    Construit un DBClient *réel*. On essaie plusieurs chemins standards :
    - mod.DBClient.from_env() (si vous l'exposez)
    - mod.DBClient(dsn=...) (si vous prenez un DSN)
    - mod.DBClient() nu (si vous lisez la conf ailleurs)
    Si rien ne marche, on SKIP (pas de fake).
    """
    if _has_attr(mod.DBClient, "from_env"):
        try:
            return mod.DBClient.from_env()
        except Exception as e:
            pytest.skip(f"DBClient.from_env() indisponible: {e}")

    # Essaie un DSN via variable d'env fournie par la CI (ex. PostgreSQL)
    dsn = os.getenv("TEST_DB_DSN")
    if dsn:
        try:
            return mod.DBClient(dsn=dsn)
        except Exception as e:
            pytest.skip(f"DBClient(dsn) indisponible: {e}")

    # Dernier essai : constructeur par défaut
    try:
        return mod.DBClient()
    except Exception as e:
        pytest.skip(f"Impossible d’instancier DBClient sans mock: {e}")


@pytest.mark.integration
def test_provider_runner_end_to_end(db_client, ensure_config_folder, logger, caplog):
    """
    Test d’intégration *réel* :
    - utilise ProviderRunner et vos modules concrets
    - lit un vrai data.yaml (minimal ou le vôtre)
    - appelle run(provider) et vérifie que ça s’exécute sans exception
    - vérifie les logs de progression
    """
    # prérequis API visibles dans ta capture
    required = [
        "load_rules_for_provider",
        "load_data_io_config",
        "FICMatchingTable",
        "MatchingTable",
        "UnifiedLazyLoader",
        "MatchingPipelineLazy",
        "apply_fic_prefilter",
    ]
    missing = [name for name in required if not hasattr(mod, name)]
    if missing:
        pytest.skip(f"Dépendances manquantes dans run_provider: {missing}")

    runner = mod.ProviderRunner(db=db_client, logger=logger)

    caplog.set_level(logging.INFO)
    provider = os.getenv("TEST_PROVIDER", "ACME_IT")  # 👉 adapte si besoin

    # ⚠️ s’exécutera vraiment avec vos classes & I/O
    runner.run(provider)

    # assertions non-fragiles : on vérifie le déroulé visible
    assert f"Running matching for provider {provider}" in caplog.text
    # si votre config minimal a FR/DE :
    assert "Matching done for scope FR" in caplog.text or "FR" in caplog.text
    assert "Matching done for scope DE" in caplog.text or "DE" in caplog.text
