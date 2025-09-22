from dataclasses import dataclass
from dotenv import load_dotenv
import os

load_dotenv()

@dataclass(frozen=True)
class VaultConfig:
    url: str
    namespace: str
    role: str
    addr: str
    auth_method: str
    service_account_token_path: str
    verify_ssl: bool = True
    request_timeout: float = 10.0

    @staticmethod
    def from_env() -> "VaultConfig":
        return VaultConfig(
            url=os.environ["VAULT_URL"],
            namespace=os.environ["VAULT_NAMESPACE"],
            role=os.environ["VAULT_ROLE"],
            addr=os.environ.get("VAULT_ADDR", os.environ.get("VAULT_ADDRESS", "")),
            auth_method=os.environ.get("VAULT_AUTH", "jwt"),
            service_account_token_path=os.environ["SERVICE_ACCOUNT_TOKEN_PATH"],
            verify_ssl=os.environ.get("VAULT_VERIFY_SSL", "true").lower() == "true",
            request_timeout=float(os.environ.get("VAULT_REQUEST_TIMEOUT", "10")),
        )

@dataclass(frozen=True)
class DbConfig:
    name: str
    host: str
    port: int
    sslmode: str

    @staticmethod
    def from_env() -> "DbConfig":
        return DbConfig(
            name=os.environ["DATABASE_NAME"],
            host=os.environ["DATABASE_HOST"],
            port=int(os.environ.get("DATABASE_PORT", "5432")),
            sslmode=os.environ.get("DATABASE_SSL_MODE", "require"),
        )



from __future__ import annotations
import os
import requests
from typing import Tuple, Optional
from ..config import VaultConfig
from ..logging_utils import setup_logger

log = setup_logger("vault")

class VaultClient:
    """
    Client HashiCorp Vault minimal pour récupérer des secrets DB.
    - Auth: JWT (k8s) via service account token.
    - read_secret: GET sur VAULT_URL (chemin complet KV v2 ou DB/creds).
    """
    def __init__(self, conf: VaultConfig, session: Optional[requests.Session] = None):
        self.conf = conf
        self._session = session or requests.Session()

    def _read_file(self, path: str) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return f.read().strip()

    def get_client_token(self) -> str:
        jwt = self._read_file(self.conf.service_account_token_path)
        url = f"{self.conf.addr}/v1/auth/{self.conf.auth_method}/login".rstrip("/")
        headers = {"X-Vault-Namespace": self.conf.namespace, "X-Vault-Request": "true"}
        payload = {"jwt": jwt, "role": self.conf.role}
        log.info("Vault login (method=%s, role=%s)", self.conf.auth_method, self.conf.role)
        r = self._session.post(
            url, json=payload, headers=headers, timeout=self.conf.request_timeout, verify=self.conf.verify_ssl
        )
        r.raise_for_status()
        token = r.json()["auth"]["client_token"]
        return token

    def read_secret(self, path_or_full_url: str, token: Optional[str] = None) -> dict:
        token = token or self.get_client_token()
        url = path_or_full_url if path_or_full_url.startswith("http") else f"{self.conf.addr}{path_or_full_url}"
        headers = {
            "X-Vault-Token": token,
            "X-Vault-Namespace": self.conf.namespace,
            "X-Vault-Request": "true",
        }
        r = self._session.get(url, headers=headers, timeout=self.conf.request_timeout, verify=self.conf.verify_ssl)
        r.raise_for_status()
        return r.json()

    def get_db_credentials(self) -> Tuple[str, str]:
        """
        Attendu: self.conf.url pointe vers un endpoint qui renvoie {data:{username, password}}
        (DB dynamic creds ou KV v2 data.username/password)
        """
        token = self.get_client_token()
        payload = self.read_secret(self.conf.url, token=token)
        # Gère KV v2 (data.data) ou DB creds (data)
        data = payload.get("data", {})
        if "data" in data and isinstance(data["data"], dict):
            data = data["data"]
        username = data["username"]
        password = data["password"]
        return username, password



from __future__ import annotations
from typing import Optional, Iterable, Tuple, Any, List, Dict
import polars as pl
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import execute_values
from datetime import datetime
from ..config import DbConfig, VaultConfig
from ..logging_utils import setup_logger
from ..security.vault_client import VaultClient

log = setup_logger("db")

class DbClient:
    """
    Client Postgres avec pool de connexions.
    - récupère les identifiants via Vault
    - expose des helpers pour lire en Polars et faire des updates batch sécurisées
    """
    def __init__(self, db_conf: DbConfig, vault_conf: VaultConfig, minconn: int = 1, maxconn: int = 5):
        self.db_conf = db_conf
        self.vault_conf = vault_conf
        self.vault = VaultClient(vault_conf)
        self._pool: Optional[SimpleConnectionPool] = None
        self._init_pool(minconn, maxconn)

    def _init_pool(self, minconn: int, maxconn: int):
        user, pwd = self.vault.get_db_credentials()
        log.info("Initializing PG pool to %s:%s/%s", self.db_conf.host, self.db_conf.port, self.db_conf.name)
        self._pool = SimpleConnectionPool(
            minconn, maxconn,
            dbname=self.db_conf.name,
            user=user,
            password=pwd,
            host=self.db_conf.host,
            port=self.db_conf.port,
            sslmode=self.db_conf.sslmode,
        )

    def _conn(self):
        if not self._pool:
            raise RuntimeError("Pool not initialized")
        return self._pool.getconn()

    def _put(self, conn):
        if self._pool:
            self._pool.putconn(conn)

    def close(self):
        if self._pool:
            self._pool.closeall()

    # ---------- Reads ----------

    def read_as_polars(self, query: str, schema: Optional[Dict[str, pl.datatypes.PolarsDataType]] = None) -> pl.DataFrame:
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                if schema:
                    return pl.DataFrame(rows, schema=schema)
                # fallback inféré
                return pl.DataFrame(rows)
        finally:
            self._put(conn)

    # wrappers spécifiques (optionnels)
    def read_norm_entity(self, query: str, schema: Dict[str, pl.datatypes.PolarsDataType]) -> pl.DataFrame:
        return self.read_as_polars(query, schema=schema)

    def read_fic(self, query: str, schema: Dict[str, pl.datatypes.PolarsDataType]) -> pl.DataFrame:
        return self.read_as_polars(query, schema=schema)

    def read_final(self, query: str, schema: Dict[str, pl.datatypes.PolarsDataType]) -> pl.DataFrame:
        return self.read_as_polars(query, schema=schema)

    # ---------- Updates ----------

    def update_cluster_ids(self, df: pl.DataFrame, table: str = "own_01188_tpn.t_norm_entity", batch_size: int = 1000) -> int:
        """
        Mise à jour sécurisée via execute_values (évite la concat SQL).
        Attend colonnes: ent_id, id_cluster, matching_rule, matching_date
        """
        if df.is_empty():
            return 0

        df = df.select(["ent_id", "id_cluster", "matching_rule", "matching_date"])\
               .with_columns([
                   pl.col("ent_id").cast(pl.Utf8),
                   pl.col("id_cluster").cast(pl.Utf8),
                   pl.col("matching_rule").cast(pl.Utf8),
                   pl.col("matching_date").cast(pl.Datetime)
               ])

        values = [tuple(row) for row in df.iter_rows()]
        updated = 0
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                tmpl = f"""
                UPDATE {table} AS t
                SET id_cluster = v.id_cluster,
                    matching_rule = v.matching_rule,
                    matching_date = v.matching_date
                FROM (VALUES %s) AS v(ent_id, id_cluster, matching_rule, matching_date)
                WHERE t.ent_id = v.ent_id
                """
                for i in range(0, len(values), batch_size):
                    batch = values[i:i+batch_size]
                    execute_values(cur, tmpl, batch)
                    updated += cur.rowcount
                conn.commit()
            log.info("Batch update done, rows=%s", updated)
        except Exception as e:
            conn.rollback()
            log.error("update_cluster_ids failed: %s", e)
            raise
        finally:
            self._put(conn)
        return updated

    def nullify_unique_clusters(self, table: str = "own_01188_tpn.t_norm_entity") -> int:
        """
        Met à NULL id_cluster/matching_rule/matching_date lorsque id_cluster apparaît une seule fois.
        """
        conn = self._conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    WITH single AS (
                        SELECT id_cluster FROM {table}
                        GROUP BY id_cluster
                        HAVING COUNT(*) = 1
                    )
                    UPDATE {table} t
                    SET id_cluster = NULL, matching_rule = NULL, matching_date = NULL
                    WHERE t.id_cluster IN (SELECT id_cluster FROM single)
                """)
                affected = cur.rowcount
                conn.commit()
                log.info("Nullified unique clusters: %s", affected)
                return affected
        except Exception as e:
            conn.rollback()
            log.error("nullify_unique_clusters failed: %s", e)
            raise
        finally:
            self._put(conn)



import json
import types
import requests
from matching_engine.config import VaultConfig
from matching_engine.security.vault_client import VaultClient

class _FakeResp:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status
    def raise_for_status(self): 
        if self.status_code >= 400: 
            raise requests.HTTPError(self.status_code)
    def json(self): 
        return self._payload

class _FakeSession:
    def __init__(self):
        self.calls = []
    def post(self, url, json=None, headers=None, timeout=None, verify=None):
        self.calls.append(("POST", url, json))
        return _FakeResp({"auth":{"client_token":"tok123"}})
    def get(self, url, headers=None, timeout=None, verify=None):
        self.calls.append(("GET", url, None))
        return _FakeResp({"data":{"username":"u","password":"p"}})

def test_vault_client_basic(tmp_path, monkeypatch):
    token_file = tmp_path / "sa.token"
    token_file.write_text("JWT_HERE", encoding="utf-8")

    conf = VaultConfig(
        url="http://vault:8200/v1/kv/dbcreds",
        namespace="ns",
        role="roleX",
        addr="http://vault:8200",
        auth_method="jwt",
        service_account_token_path=str(token_file),
        verify_ssl=False,
        request_timeout=1.0,
    )

    sess = _FakeSession()
    vc = VaultClient(conf, session=sess)

    u, p = vc.get_db_credentials()
    assert u == "u" and p == "p"
    assert sess.calls[0][0] == "POST"
    assert sess.calls[1][0] == "GET"


import polars as pl
import types
from matching_engine.config import DbConfig, VaultConfig
from matching_engine.db.db_client import DbClient

class _FakePool:
    def __init__(self, conn):
        self.conn = conn
    def getconn(self): 
        return self.conn
    def putconn(self, conn): 
        pass
    def closeall(self): 
        pass

class _FakeCursor:
    def __init__(self):
        self.queries = []
        self._rows = []
        self.rowcount = 0
    def execute(self, q, params=None):
        self.queries.append((q, params))
        if "SELECT" in q:
            self._rows = [(1,)]
        elif "UPDATE" in q:
            self.rowcount = 3
    def fetchall(self):
        return self._rows
    def __enter__(self): return self
    def __exit__(self, *a): pass

class _FakeConn:
    def __init__(self):
        self.cur = _FakeCursor()
        self.commits = 0
        self.rollbacks = 0
    def cursor(self): 
        return self.cur
    def commit(self): 
        self.commits += 1
    def rollback(self): 
        self.rollbacks += 1

def test_db_client_read_and_update(monkeypatch):
    # Fake vault returns credentials without calling network
    class _FakeVault:
        def get_db_credentials(self):
            return "user", "pwd"

    # Monkeypatch DbClient internals to avoid real pool
    def _init_pool(self, minconn, maxconn):
        self._pool = _FakePool(_FakeConn())
    monkeypatch.setattr(DbClient, "_init_pool", _init_pool)
    DbClient.vault = _FakeVault()

    dbc = DbClient(DbConfig(name="db", host="h", port=5432, sslmode="disable"),
                   VaultConfig(url="", namespace="", role="", addr="", auth_method="", service_account_token_path=""))
    # read
    df = dbc.read_as_polars("SELECT 1 AS x", schema={"x": pl.Int64})
    assert df.shape == (1,1)

    # update_cluster_ids
    updates = pl.DataFrame({
        "ent_id": ["e1","e2","e3"],
        "id_cluster": ["c1","c2","c3"],
        "matching_rule": ["L1","L2","L3"],
        "matching_date": [None, None, None],
    })
    n = dbc.update_cluster_ids(updates)
    assert n == 3

    # nullify_unique_clusters
    n2 = dbc.nullify_unique_clusters()
    assert n2 == 3

    dbc.close()

