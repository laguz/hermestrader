import datetime
import os
import logging
from cryptography.fernet import Fernet

from hermes.clock import Clock, RealClock, SimulatedClock

_GLOBAL_CLOCK: Clock = RealClock()

def utc_now() -> datetime.datetime:
    """Return timezone-naive current UTC time, honoring virtual time if set."""
    return _GLOBAL_CLOCK.utc_now()

def date_today() -> datetime.date:
    """Return current date, honoring virtual time if set."""
    return _GLOBAL_CLOCK.date_today()

def now(tz: datetime.tzinfo | None = None) -> datetime.datetime:
    """Return current local/aware time, honoring virtual time if set."""
    return _GLOBAL_CLOCK.now(tz)

def set_virtual_time(dt: datetime.datetime | None) -> None:
    """Set the virtual time for testing or simulation mode.

    Pass None to reset and restore system time tracking.
    """
    global _GLOBAL_CLOCK
    if dt is None:
        _GLOBAL_CLOCK = RealClock()
    else:
        _GLOBAL_CLOCK = SimulatedClock(dt)



logger = logging.getLogger("hermes.utils")

ENCRYPTION_KEY = os.environ.get("HERMES_ENCRYPTION_KEY")
ENCRYPTED_PREFIX = "enc:"

def _get_fernet() -> Fernet | None:
    if not ENCRYPTION_KEY:
        return None
    try:
        return Fernet(ENCRYPTION_KEY.encode())
    except Exception as exc:
        logger.warning("Invalid HERMES_ENCRYPTION_KEY: %s", exc)
        return None

def encrypt_value(value: str) -> str:
    """Encrypt a string value using Fernet if HERMES_ENCRYPTION_KEY is set.

    Returns the encrypted value with an 'enc:' prefix, or the original
    value if encryption is disabled.
    """
    if not value:
        return value

    f = _get_fernet()
    if not f:
        return value

    try:
        encrypted = f.encrypt(value.encode()).decode()
        return f"{ENCRYPTED_PREFIX}{encrypted}"
    except Exception as exc:
        logger.warning("Encryption failed: %s", exc)
        return value

def decrypt_value(value: str) -> str:
    """Decrypt a string value if it has the 'enc:' prefix.

    Returns the decrypted value, or the original value if it's not
    prefixed or if decryption fails.
    """
    if not value or not value.startswith(ENCRYPTED_PREFIX):
        return value

    f = _get_fernet()
    if not f:
        logger.warning("Value is encrypted but HERMES_ENCRYPTION_KEY is missing")
        return value

    try:
        token = value[len(ENCRYPTED_PREFIX):]
        return f.decrypt(token.encode()).decode()
    except Exception as exc:
        logger.warning("Decryption failed: %s", exc)
        return value


def check_for_updates() -> None:
    """Check for updates to Hermes by querying GitHub.
    
    Checks the remote VERSION and the latest commit details on the active branch
    (main for paper, live for live mode). Writes the results to the database
    as a JSON-serialized update_status setting.
    """
    import json
    import requests
    from datetime import datetime, timezone
    from hermes.db.models import HermesDB
    from hermes.service2_watcher._app_state import read_version

    mode = os.environ.get("HERMES_MODE", "paper").lower().strip()
    branch = "live" if mode == "live" else "main"
    local_version = read_version()

    logger.info("Starting startup update check for mode=%s (branch=%s), local version=%s...", mode, branch, local_version)

    update_available = False
    remote_version = local_version
    latest_commit_sha = ""
    latest_commit_msg = ""
    error_msg = None

    # 1. Fetch remote VERSION from GitHub
    try:
        ver_url = f"https://raw.githubusercontent.com/laguz/hermestrader/{branch}/VERSION"
        res = requests.get(ver_url, timeout=10)
        if res.status_code == 200:
            remote_version = res.text.strip()
            if remote_version != local_version and local_version != "dev":
                update_available = True
                logger.warning(
                    "[UPDATE AVAILABLE] A newer version of Hermes is available! "
                    "Local version: %s, Remote version: %s. Run ./hermes.sh update to pull.",
                    local_version, remote_version
                )
        else:
            logger.warning("Could not fetch remote version, status code: %d", res.status_code)
    except Exception as e:
        logger.warning("Failed to fetch remote version from GitHub: %s", e)
        error_msg = str(e)

    # 2. Fetch latest commit from GitHub to display details
    try:
        commit_url = f"https://api.github.com/repos/laguz/hermestrader/commits/{branch}"
        headers = {"User-Agent": "HermesTrader-App"}
        res = requests.get(commit_url, headers=headers, timeout=10)
        if res.status_code == 200:
            commit_data = res.json()
            latest_commit_sha = commit_data.get("sha", "")[:8]
            commit_obj = commit_data.get("commit", {})
            latest_commit_msg = commit_obj.get("message", "").split("\n")[0]
            logger.info("Latest remote commit on %s: %s - %s", branch, latest_commit_sha, latest_commit_msg)
        else:
            logger.warning("Could not fetch latest commit, status code: %d", res.status_code)
    except Exception as e:
        logger.warning("Failed to fetch latest commit from GitHub: %s", e)

    try:
        dsn = os.environ.get("HERMES_DSN", "postgresql+psycopg://hermes:hermes@db:5432/hermes")
        db = HermesDB(dsn)
        payload = {
            "update_available": update_available,
            "local_version": local_version,
            "remote_version": remote_version,
            "latest_commit_sha": latest_commit_sha,
            "latest_commit_msg": latest_commit_msg,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "error": error_msg
        }
        import asyncio
        asyncio.run(db.set_setting("update_status", json.dumps(payload)))
        asyncio.run(db.write_log("ENGINE", f"Startup update check completed. Update available: {update_available}"))
    except Exception as e:
        logger.warning("Failed to save update status to DB: %s", e)


async def sync_soul_file_to_db(db) -> None:
    """Read the content of the mounted soul.md file and sync it to the DB if different.
    
    This acts as the source-of-truth loader on container startup.
    """
    soul_path = os.environ.get("HERMES_SOUL_PATH", "/app/soul.md")
    if os.path.exists(soul_path):
        try:
            with open(soul_path, "r", encoding="utf-8") as f:
                file_soul = f.read()
            db_soul = (await db.get_setting("soul_md")) or ""
            if file_soul != db_soul:
                logger.info("Syncing soul.md from file to database (length: %d)", len(file_soul))
                await db.set_setting("soul_md", file_soul)
                await db.write_log("ENGINE", f"Loaded soul.md from host repository file into database ({len(file_soul.encode())}B)")
        except Exception as exc:
            logger.warning("Failed to sync soul.md to database: %s", exc)
