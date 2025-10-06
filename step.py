# tests/test_db_client.py
import builtins
from types import SimpleNamespace
import polars as pl
import pytest

# 👉 adapte ce chemin d'import si ton module n'est pas à la racine
import db_client as mod


# -----------------------------
# Doubles (mocks/fakes) locaux
# -----------------------------
class FakeCursor:
    def __init__(self, rows=None, cols=None):
        self._rows = rows or []
        self._cols = cols or []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        # on ne fait que mémoriser ce qui est passé
        self.last_query = query
        self.last_params = params

        # Simule une description de colonnes comme psycopg2: tuple dont [0] = nom
        self.description = [(c,) for c in self._cols]

    def fetchall(self):
        return self._rows

    def executemany(self, query, params_seq):
        self.last_query = query
        self.last_params = list(params_seq)


class FakeConnection:
    def __init__(self, cursor: FakeCursor):
        self._cursor = cursor
        self.commits = 0

    def cursor(self):
        return self._cursor

    def commit(self):
        self.commits += 1


class FakePool:
    def __init__(self, *args, **kwargs):
        # Mémorise les args passés pour les assertions
        self.args = args
        self.kwargs = kwargs
        self._conn = None

    def set_connection(self, conn):
        self._conn = conn

    def getconn(self):
        if not self._conn:
            raise RuntimeError("no fake connection set")
        return self._conn

    def putconn(self, conn):
        # pas d'action, juste pour tracer
        self._put_called_with = conn

    def closeall(self):
        self._closed = True


class FakeVaultClient:
    def __init__(self, _):
        pass

    def get_db_credentials(self):
        return "db_user", "db_pwd"


# -----------------------------
# Fixtures de patch/mocks
# -----------------------------
@pytest.fixture(autouse=True)
def patch_external_classes(monkeypatch):
    """
    Remplace dans le module testé:
      - VaultClient par FakeVaultClient
      - SimpleConnectionPool par FakePool
    """
    monkeypatch.setattr(mod, "VaultClient", FakeVaultClient, raising=True)
    monkeypatch.setattr(mod, "SimpleConnectionPool", FakePool, raising=True)


@pytest.fixture
def db_conf():
    # Remplace les dataclasses par un SimpleNamespace avec les mêmes attributs attendus
    return SimpleNamespace(
        host="localhost",
        port=5432,
        name="test_db",
        sslmode="require",
    )


@pytest.fixture
def vault_conf():
    return SimpleNamespace(path="secret/path")


@pytest.fixture
def client(db_conf, vault_conf):
    return mod.DbClient(db_conf=db_conf, vault_conf=vault_conf)


# -----------------------------
# Tests
# -----------------------------
def test_init_pool_creates_pool_and_uses_vault_credentials(client, db_conf, monkeypatch):
    client._init_pool(minconn=1, maxconn=5)

    # La pool a bien été créée
    assert isinstance(client._pool, FakePool)

    # Vérifie que les bons paramètres DB ont été passés au pool
    kwargs = client._pool.kwargs
    assert kwargs["dbname"] == db_conf.name
    assert kwargs["user"] == "db_user"
    assert kwargs["password"] == "db_pwd"
    assert kwargs["host"] == db_conf.host
    assert kwargs["port"] == db_conf.port
    # sslmode peut ne pas exister selon ton code exact; adapte si besoin
    assert kwargs.get("sslmode") in (None, db_conf.sslmode)


def test_conn_raises_when_pool_not_initialized(client):
    with pytest.raises(RuntimeError, match="pool not initialized"):
        client._conn()  # méthode privée vue sur la capture


def test_put_and_close_delegate_to_pool(client):
    client._init_pool(1, 5)
    fake_cursor = FakeCursor(rows=[], cols=["a"])
    fake_conn = FakeConnection(fake_cursor)
    client._pool.set_connection(fake_conn)

    # get/put
    c = client._conn()
    client._put(c)
    assert getattr(client._pool, "_put_called_with", None) is c

    # close
    client.close()
    assert getattr(client._pool, "_closed", False) is True


def test_read_as_polars_without_params_and_without_schema(client):
    # Prépare une connexion renvoyant des lignes + colonnes
    rows = [(1, "x"), (2, "y")]
    cols = ["id", "label"]

    client._init_pool(1, 5)
    fake_cursor = FakeCursor(rows=rows, cols=cols)
    fake_conn = FakeConnection(fake_cursor)
    client._pool.set_connection(fake_conn)

    df = client.read_as_polars("SELECT * FROM t", schema=None, params=None)

    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, 2)
    assert set(df.columns) == set(cols)


def test_read_as_polars_with_params_and_schema_cast(client):
    rows = [("3", "42.5"), ("4", "13.0")]  # strings pour tester le cast
    cols = ["id", "value"]

    client._init_pool(1, 5)
    fake_cursor = FakeCursor(rows=rows, cols=cols)
    fake_conn = FakeConnection(fake_cursor)
    client._pool.set_connection(fake_conn)

    schema = {"id": pl.Int64, "value": pl.Float64}
    df = client.read_as_polars(
        "SELECT id, value FROM t WHERE id >= %s",
        schema=schema,
        params=(3,),
    )

    # Vérifie que l'exécution a bien reçu les params
    assert fake_cursor.last_params == (3,)

    # Vérifie le cast réalisé par with_columns(...)
    assert df["id"].dtype == pl.Int64
    assert df["value"].dtype == pl.Float64
    assert df.shape == (2, 2)


def test_insert_records_executemany_and_commit(client, capsys):
    client._init_pool(1, 5)
    fake_cursor = FakeCursor()
    fake_conn = FakeConnection(fake_cursor)
    client._pool.set_connection(fake_conn)

    params = [{"a": 1}, {"a": 2}, {"a": 3}]
    client.insert_records("INSERT ...", params)

    assert fake_cursor.last_query.startswith("INSERT")
    assert fake_cursor.last_params == params
    assert fake_conn.commits == 1

    out = capsys.readouterr().out
    assert "Inserted 3 new FIC records with clusters." in out
