import pytest
import polars as pl

import db_client


# ---------- Fakes / Mocks ----------
class FakeVaultClient:
    def __init__(self, conf):
        self.conf = conf

    def get_db_credentials(self):
        return "user_test", "pwd_test"


class FakePool:
    def __init__(self, minconn, maxconn, **kwargs):
        self.minconn = minconn
        self.maxconn = maxconn
        self.kwargs = kwargs
        self.conn = FakeConn()
        self.closed = False

    def getconn(self):
        return self.conn

    def putconn(self, conn):
        self.put_called = True

    def closeall(self):
        self.closed = True


class FakeCursor:
    def __init__(self, rows=None):
        self.rows = rows or [(1, "a"), (2, "b")]
        self.description = [("id",), ("name",)]

    def execute(self, query, params=None):
        self.executed = (query, params)

    def fetchall(self):
        return self.rows

    def executemany(self, query, params):
        self.executemany_called = (query, params)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass


class FakeConn:
    def __init__(self):
        self.cursor_obj = FakeCursor()
        self.committed = False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.committed = True


# ---------- Fixtures ----------
@pytest.fixture(autouse=True)
def patch_dependencies(monkeypatch):
    monkeypatch.setattr(db_client, "VaultClient", FakeVaultClient)
    monkeypatch.setattr(db_client, "SimpleConnectionPool", FakePool)


@pytest.fixture
def db_conf():
    class Conf:
        host = "localhost"
        port = 5432
        name = "test_db"
        sslmode = "require"
    return Conf()


@pytest.fixture
def vault_conf():
    class VaultConf:
        path = "secret/path"
    return VaultConf()


@pytest.fixture
def client(db_conf, vault_conf):
    return db_client.DbClient(db_conf=db_conf, vault_conf=vault_conf)


# ---------- Tests ----------

def test_init_pool_creates_pool(client):
    client._init_pool(minconn=1, maxconn=5)
    pool = client._pool
    assert isinstance(pool, FakePool)
    assert pool.kwargs["dbname"] == client.db_conf.name
    assert pool.kwargs["user"] == "user_test"
    assert pool.kwargs["password"] == "pwd_test"


def test_conn_and_put(client):
    client._init_pool(1, 5)
    conn = client._conn()
    assert isinstance(conn, FakeConn)
    client._put(conn)
    assert getattr(client._pool, "put_called", False) is True


def test_close_closes_pool(client):
    client._init_pool(1, 5)
    client.close()
    assert client._pool.closed is True


def test_conn_without_pool_raises(client):
    with pytest.raises(RuntimeError):
        client._conn()


def test_read_as_polars_basic(client):
    client._init_pool(1, 5)
    df = client.read_as_polars("SELECT * FROM t")
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (2, 2)
    assert "id" in df.columns


def test_read_as_polars_with_schema_and_params(client):
    client._init_pool(1, 5)
    schema = {"id": pl.Int64, "name": pl.Utf8}
    df = client.read_as_polars("SELECT * FROM t WHERE id=%s", schema=schema, params=(1,))
    assert df["id"].dtype == pl.Int64
    assert df["name"].dtype == pl.Utf8


def test_insert_records_executes_and_commits(client, capsys):
    client._init_pool(1, 5)
    params = [{"a": 1}, {"a": 2}]
    client.insert_records("INSERT INTO test VALUES (%s)", params)
    conn = client._pool.conn
    cur = conn.cursor_obj
    assert cur.executemany_called[1] == params
    assert conn.committed is True
    out = capsys.readouterr().out
    assert "Inserted" in out
