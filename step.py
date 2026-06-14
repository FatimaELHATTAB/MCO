import threading
import time
from psycopg2.pool import SimpleConnectionPool


def _start_refresh_loop_if_needed(self) -> None:
    """
    Start a background thread that refreshes the DB credentials
    before the Vault lease expires.

    This method is safe to call multiple times.
    The refresh thread will be started only once.
    """
    if self._refresh_thread_started:
        return

    self._refresh_thread_started = True

    thread = threading.Thread(
        target=self._refresh_loop,
        daemon=True,
        name="vault-db-credentials-refresh",
    )
    thread.start()

    log.info("Vault DB credentials refresh thread started")


def _refresh_loop(self) -> None:
    """
    Background loop responsible for refreshing DB credentials.

    It sleeps for around 80% of the Vault lease duration,
    then recreates the PostgreSQL pool with fresh credentials.
    """
    while True:
        try:
            creds = self._db_credentials

            if creds and creds.lease_duration:
                sleep_seconds = int(creds.lease_duration * 0.80)
            else:
                # Fallback if Vault does not return a lease duration
                sleep_seconds = 24 * 3600

            log.info(
                "Next Vault DB credentials refresh scheduled in %s seconds",
                sleep_seconds,
            )

            time.sleep(sleep_seconds)

            self._recreate_pool_with_new_credentials()

        except Exception as exc:
            log.exception(
                "Error while refreshing Vault DB credentials: %s",
                exc,
            )

            # Avoid tight infinite loop in case of repeated errors
            time.sleep(3600)


def _recreate_pool_with_new_credentials(self) -> None:
    """
    Retrieve fresh DB credentials from Vault and recreate the PostgreSQL pool.
    """
    log.info("Refreshing database credentials from Vault")

    old_pool = self._pool

    creds = self.vault.get_db_credentials()
    self._db_credentials = creds

    new_pool = SimpleConnectionPool(
        minconn=self.minconn,
        maxconn=self.maxconn,
        host=self.db_conf.host,
        port=self.db_conf.port,
        dbname=self.db_conf.name,
        user=creds.username,
        password=creds.password,
        sslmode=self.db_conf.sslmode,
    )

    self._pool = new_pool

    if old_pool:
        old_pool.closeall()

    log.info(
        "Database credentials refreshed successfully. New lease duration: %s seconds",
        creds.lease_duration,
    )



def get_db_credentials(self) -> DbCredentials:
    """
    Retrieve dynamic database credentials from Vault.

    Returns:
        DbCredentials: username, password and Vault lease metadata.
    """

    token = self.get_client_token()

    payload = self.read_secret(
        path_or_full_url=self.conf.url,
        token=token,
    )

    # Selon la forme de réponse Vault, les credentials peuvent être
    # soit dans payload["data"], soit dans payload["data"]["data"].
    data = payload.get("data", {})

    if "data" in data:
        secret_data = data["data"]
    else:
        secret_data = data

    username = secret_data.get("username")
    password = secret_data.get("password")

    if not username or not password:
        raise ValueError("Unable to retrieve database username/password from Vault")

    lease_id = payload.get("lease_id") or data.get("lease_id")
    lease_duration = payload.get("lease_duration") or data.get("lease_duration")
    renewable = payload.get("renewable") or data.get("renewable") or False

    if lease_duration is not None:
        lease_duration = int(lease_duration)

    return DbCredentials(
        username=username,
        password=password,
        lease_id=lease_id,
        lease_duration=lease_duration,
        renewable=renewable,
    )
