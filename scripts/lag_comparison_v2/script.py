import os
import json
import re
import ssl
import warnings
import logging
import sys
import urllib3
import pandas as pd
import pyodbc
import clickhouse_connect
from dotenv import load_dotenv
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======================================================
# Paths
# ======================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.dirname(BASE_DIR)
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")
# Shared secrets for all scripts: scripts/.env
load_dotenv(os.path.join(SCRIPTS_DIR, ".env"))

# ======================================================
# ClickHouse Environments (Green → Blue → Yellow)
# ======================================================
CLICKHOUSE_ENVIRONMENTS = {
    "🟢 Green Environment": {
        "host": "ch-prod-green.callcourier.com.pk",
        "port": 443,
    },
    "🔵 Blue Environment": {
        "host": "ch-new.callcourier.com.pk",
        "port": 443,
    },
    "🟡 Yellow Environment": {
        "host": "ch-yellow.callcourier.com.pk",
        "port": 443,
    },
}

# ======================================================
# Thread / Worker Settings
# ======================================================
CH_ENV_WORKERS = len(CLICKHOUSE_ENVIRONMENTS)
CH_TABLE_WORKERS = (os.cpu_count() or 4) * 2
SQL_DB_WORKERS = 8
PROGRESS_LOG_EVERY = 25  # emit a log line every N tables (Airflow-safe)

# ClickHouse L2 replica table name suffix
# SQL GoGreen.dbo.CNTrack → CH LOGISTICS_V2.GG_dbo_CNTrack_l2
# (some tables use a versioned suffix, e.g. GG_dbo_CNTrack_l2_v1)
CH_TABLE_SUFFIX = "_l2"
L2_SUFFIX_RE = re.compile(r"_l2(?:_v\d+)?$", re.IGNORECASE)


def is_l2_table(name: str) -> bool:
    """True for *_l2 and versioned *_l2_vN (case-insensitive)."""
    return bool(L2_SUFFIX_RE.search(name))


def strip_l2_suffix(name: str) -> str | None:
    """GG_dbo_CNTrack_l2_v1 → GG_dbo_CNTrack; None if not an L2 table."""
    m = L2_SUFFIX_RE.search(name)
    if not m:
        return None
    return name[: m.start()]

# ======================================================
# Global Settings
# ======================================================
os.environ["CLICKHOUSE_CONNECT_DISABLE_SSL_VERIFY"] = "1"
warnings.filterwarnings("ignore")
urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context

WEBHOOK_URL = (
    os.getenv("GOOGLE_CHAT_WEBHOOK_V2_URL")
    or os.getenv("GOOGLE_CHAT_WEBHOOK_URL")
    or ""
)

PKT = ZoneInfo("Asia/Karachi")

# ======================================================
# Logging
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    force=True,
    stream=sys.stdout,
)
logger = logging.getLogger("SQL_CH_COMPARE")


def log_table_progress(label: str, done: int, total: int) -> None:
    """Log progress as plain lines — tqdm bars don't render in Airflow logs."""
    if total <= 0:
        return
    pct = int(100 * done / total)
    if done == 1 or done == total or done % PROGRESS_LOG_EVERY == 0:
        logger.info("%s — %d/%d tables (%d%%)", label, done, total, pct)


# ======================================================
# Fetch retries (HTTP / transport)
# ======================================================
CH_TIMEOUTS = (300, 600, 900)
SQL_TIMEOUTS = (60, 120, 180)
_RETRYABLE_ERROR_MARKERS = (
    "unexpected http driver exception",
    "timeout",
    "timed out",
    "connection reset",
    "broken pipe",
    "connection aborted",
    "connection refused",
    "communication link failure",
    "server has gone away",
    "hyt00",
    "hyt01",
    "08s01",
)


def _exc_text(exc: BaseException) -> str:
    return str(exc).strip() or type(exc).__name__


def _is_retryable_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return any(marker in msg for marker in _RETRYABLE_ERROR_MARKERS)


def _run_with_retries(kind: str, target: str, timeouts: tuple[int, ...], call):
    """
    call(timeout_seconds) -> result.
    Retries retryable HTTP/transport errors with the next (longer) timeout.
    """
    last_exc: BaseException | None = None
    for attempt, timeout in enumerate(timeouts, start=1):
        try:
            return call(timeout)
        except Exception as exc:
            last_exc = exc
            if attempt < len(timeouts) and _is_retryable_error(exc):
                logger.warning(
                    "%s retry %d/%d for %s after %s (timeout=%ds)",
                    kind,
                    attempt + 1,
                    len(timeouts),
                    target,
                    _exc_text(exc),
                    timeouts[attempt],
                )
                continue
            raise
    raise last_exc  # pragma: no cover

# ======================================================
# Config
# ======================================================
class Config:
    DB_ABBREVIATION = {
        "HRM": "HRM",
        "GoGreen": "GG",
        "Cloud_GoGreen": "CGG",
        "SharedObject": "SO",
    }
    ABBR_TO_DATABASE = {v: k for k, v in DB_ABBREVIATION.items()}

    def __init__(self, ch_host, ch_port):
        self.clickhouse = {
            "host": ch_host,
            "port": ch_port,
            "username": os.getenv("CLICKHOUSE_V2_USER") or os.getenv("CLICKHOUSE_USER"),
            "password": os.getenv("CLICKHOUSE_V2_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD"),
            "database": os.getenv("CLICKHOUSE_V2_DATABASE") or os.getenv("CLICKHOUSE_DATABASE"),
        }
        self.mssql = {
            "server": os.getenv("MSSQL_SERVER"),
            "databases": os.getenv("MSSQL_DATABASES").split(","),
            "username": os.getenv("MSSQL_USER"),
            "password": os.getenv("MSSQL_PASSWORD"),
            "driver": os.getenv("MSSQL_DRIVER"),
        }

# ======================================================
# Size-threshold config (config.json)
# ======================================================
class SizeThresholdConfig:
    @staticmethod
    def load() -> list[dict]:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            data = json.load(f)
        tiers = data["size_thresholds"]
        if not tiers:
            raise ValueError("config.json size_thresholds is empty")
        return tiers

    @staticmethod
    def classify(sql_count: int, tiers: list[dict]) -> tuple[str, int]:
        """Return (size_name, lag_limit) for a SQL row count."""
        for tier in tiers:
            max_rows = tier["max_rows"]
            if max_rows is None or sql_count < int(max_rows):
                return tier["name"], int(tier["lag_limit"])
        last = tiers[-1]
        return last["name"], int(last["lag_limit"])


# ======================================================
# CH table name helpers
# ======================================================
def parse_ch_table_name(ch_name: str) -> tuple[str, str, str] | None:
    """
    Parse GG_dbo_CNTrack_l2 or GG_dbo_CNTrack_l2_v1 → (GoGreen, dbo, CNTrack).
    Schema is the first segment after the abbreviation; the rest is the table.
    """
    base = strip_l2_suffix(ch_name)
    if base is None:
        return None
    # Longest abbreviation first (CGG before GG)
    for abbr in sorted(Config.ABBR_TO_DATABASE.keys(), key=len, reverse=True):
        prefix = abbr + "_"
        if base.startswith(prefix):
            rest = base[len(prefix) :]
            schema, sep, table = rest.partition("_")
            if not sep or not schema or not table:
                return None
            return Config.ABBR_TO_DATABASE[abbr], schema, table
    return None


def ch_name_from_sql(database: str, schema: str, table: str) -> str | None:
    """Default CH L2 name (unversioned *_l2). Prefer keeping the real CH name when known."""
    abbr = Config.DB_ABBREVIATION.get(database)
    if not abbr:
        return None
    return f"{abbr}_{schema}_{table}{CH_TABLE_SUFFIX}"


# ======================================================
# ClickHouse Client
# ======================================================
def _make_ch_client(cfg: dict, timeout: int = CH_TIMEOUTS[0]):
    """
    Always create a FRESH clickhouse_connect client.
    Never share one client across threads — the library is not thread-safe.
    """
    return clickhouse_connect.get_client(
        host=cfg["host"],
        port=cfg["port"],
        username=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        secure=True,   # hosts use HTTPS :443
        verify=False,
        send_receive_timeout=timeout,
    )


def _close_quietly(client) -> None:
    try:
        client.close()
    except Exception:
        pass


class ClickHouseClient:
    def __init__(self, config):
        self.cfg = config.clickhouse

    def _meta_query(self, sql: str, parameters: dict, target: str):
        def _do(timeout: int):
            client = _make_ch_client(self.cfg, timeout=timeout)
            try:
                return client.query(sql, parameters=parameters).result_rows
            finally:
                _close_quietly(client)

        return _run_with_retries("CH metadata", target, CH_TIMEOUTS, _do)

    def _get_table_engines(self) -> dict[str, str]:
        """Returns {table_name_lower: engine_name}."""
        rows = self._meta_query(
            "SELECT name, engine FROM system.tables WHERE database = %(db)s",
            {"db": self.cfg["database"]},
            "system.tables",
        )
        return {name.lower(): engine for name, engine in rows}

    def _get_count_columns(self, table_names: list[str]) -> dict[str, str]:
        """
        Pick one countable column per table from system.columns.
        Prefer the first non-__deleted column; fall back to first column.
        """
        if not table_names:
            return {}

        rows = self._meta_query(
            """
            SELECT table, name, position
            FROM system.columns
            WHERE database = %(db)s AND table IN %(tables)s
            ORDER BY table, position
            """,
            {"db": self.cfg["database"], "tables": tuple(table_names)},
            "system.columns",
        )

        by_table: dict[str, list[str]] = {}
        for table, col, _pos in rows:
            by_table.setdefault(table, []).append(col)

        chosen: dict[str, str] = {}
        for table, cols in by_table.items():
            non_deleted = [c for c in cols if c != "__deleted"]
            chosen[table] = non_deleted[0] if non_deleted else cols[0]
        return chosen

    def _count_one_table(
        self, name: str, column: str, use_final: bool
    ) -> tuple[str, int | None, str | None]:
        """
        Count rows using a single column: count(col).
        Each attempt uses its own client and a longer timeout on retry.
        Returns (name, count, error) — error is set instead of a fake 0.
        """
        db = self.cfg["database"]
        if use_final:
            query = (
                f"SELECT count(`{column}`) FROM {db}.`{name}` "
                f"FINAL WHERE __deleted='false'"
            )
        else:
            query = (
                f"SELECT count(`{column}`) FROM {db}.`{name}` "
                f"WHERE __deleted='false'"
            )

        def _do(timeout: int) -> int:
            client = _make_ch_client(self.cfg, timeout=timeout)
            try:
                return int(client.query(query).result_rows[0][0])
            finally:
                _close_quietly(client)

        try:
            cnt = _run_with_retries("CH count", name, CH_TIMEOUTS, _do)
            return name, cnt, None
        except Exception as exc:
            err = _exc_text(exc)
            logger.error(
                "CH count failed for %s after %d attempts: %s",
                name,
                len(CH_TIMEOUTS),
                err,
            )
            return name, None, err

    def list_l2_tables(self) -> list[tuple[str, bool]]:
        """Return [(table_name, use_final), ...] for *_l2 / *_l2_vN tables only."""
        engines = self._get_table_engines()
        rows = self._meta_query(
            "SELECT name FROM system.tables WHERE database = %(db)s",
            {"db": self.cfg["database"]},
            "system.tables",
        )

        work = []
        for (name,) in rows:
            t = name.lower()
            if t.endswith(("_kafka", "_mv")) or t.startswith("vw_") or not is_l2_table(t):
                continue
            engine = engines.get(t, "")
            use_final = engine.lower() == "replacingmergetree"
            work.append((name, use_final))
        return work

    def prepare_tables(
        self, label: str | None = None
    ) -> tuple[list[tuple[str, str, bool]], list[str], dict[str, str]]:
        """
        Metadata only: L2 list + count columns.
        Returns (jobs, names, errors) where each job is (name, column, use_final).
        """
        progress_label = label or f"CH [{self.cfg['host'].split('.')[0]}]"
        try:
            work = self.list_l2_tables()
        except Exception as exc:
            err = _exc_text(exc)
            logger.error("CH metadata failed for %s: %s", progress_label, err)
            return [], [], {"(metadata)": err}

        names = [name for name, _ in work]
        if not work:
            return [], [], {}

        try:
            count_cols = self._get_count_columns(names)
        except Exception as exc:
            err = _exc_text(exc)
            logger.error("CH metadata failed for %s: %s", progress_label, err)
            return [], names, {"(metadata)": err}

        jobs: list[tuple[str, str, bool]] = []
        errors: dict[str, str] = {}
        for name, use_final in work:
            col = count_cols.get(name)
            if not col:
                errors[name] = "no countable column found"
                logger.error("CH count failed for %s: no countable column found", name)
                continue
            jobs.append((name, col, use_final))
        return jobs, names, errors

    def count_prepared(
        self,
        jobs: list[tuple[str, str, bool]],
        label: str | None = None,
    ) -> tuple[dict[str, int], dict[str, str]]:
        progress_label = label or f"CH [{self.cfg['host'].split('.')[0]}]"
        if not jobs:
            return {}, {}

        data: dict[str, int] = {}
        errors: dict[str, str] = {}
        with ThreadPoolExecutor(max_workers=CH_TABLE_WORKERS) as pool:
            futures = {
                pool.submit(self._count_one_table, name, col, use_final): name
                for name, col, use_final in jobs
            }
            done = 0
            total = len(futures)
            logger.info("%s — counting %d table(s)…", progress_label, total)
            for fut in as_completed(futures):
                t_name, cnt, err = fut.result()
                if err:
                    errors[t_name] = err
                else:
                    data[t_name] = cnt
                done += 1
                log_table_progress(progress_label, done, total)

        return data, errors

# ======================================================
# SQL Server Client (only tables present in ClickHouse)
# ======================================================
class SQLServerClient:
    def __init__(self, config):
        self.cfg = config.mssql

    def _connect(self, db: str, timeout: int):
        conn = pyodbc.connect(
            f"DRIVER={{{self.cfg['driver']}}};"
            f"SERVER={self.cfg['server']};DATABASE={db};"
            f"UID={self.cfg['username']};PWD={self.cfg['password']};"
            f"Encrypt=yes;TrustServerCertificate=yes;"
            f"Connection Timeout={timeout};",
            timeout=timeout,
        )
        conn.timeout = timeout
        return conn

    def _fetch_one_db(
        self, db: str, wanted: dict[tuple[str, str], list[str]]
    ) -> tuple[dict[str, int], str | None]:
        """
        Fetch row counts for specific (schema, table) pairs in one MSSQL database.
        wanted maps (schema, table) → list of ClickHouse table names to key the result.
        Returns ({CH_table_name: sql_count}, error).
        """
        if not wanted:
            return {}, None

        # Build parameterized IN list of (schema, table)
        pairs = sorted(wanted.keys())
        placeholders = ",".join("(?, ?)" for _ in pairs)
        query = f"""
        SELECT s.name schema_name, t.name table_name, SUM(p.rows) row_count
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.partitions p ON p.object_id = t.object_id
        WHERE p.index_id IN (0, 1)
          AND EXISTS (
              SELECT 1
              FROM (VALUES {placeholders}) AS w(schema_name, table_name)
              WHERE w.schema_name = s.name AND w.table_name = t.name
          )
        GROUP BY s.name, t.name
        """
        params: list = []
        for schema, table in pairs:
            params.extend([schema, table])

        def _do(timeout: int) -> dict[str, int]:
            conn = self._connect(db, timeout)
            try:
                df = pd.read_sql(query, conn, params=params)
            finally:
                conn.close()

            out: dict[str, int] = {}
            for _, r in df.iterrows():
                key = (r["schema_name"], r["table_name"])
                sql_count = int(r["row_count"])
                for ch_name in wanted.get(key, []):
                    out[ch_name] = sql_count
            return out

        try:
            return _run_with_retries("SQL fetch", db, SQL_TIMEOUTS, _do), None
        except Exception as exc:
            err = _exc_text(exc)
            logger.error(
                "SQL fetch failed for %s after %d attempts: %s",
                db,
                len(SQL_TIMEOUTS),
                err,
            )
            return {}, err

    def fetch_for_ch_tables(
        self, ch_table_names: set[str]
    ) -> tuple[dict[str, int], dict[str, str]]:
        """
        Resolve CH names to SQL db/schema/table and fetch only those counts.
        Returns ({CH_table_name: sql_count}, {CH_table_name: error}).
        """
        by_db: dict[str, dict[tuple[str, str], list[str]]] = {}
        for ch_name in ch_table_names:
            parsed = parse_ch_table_name(ch_name)
            if not parsed:
                continue
            db, schema, table = parsed
            if db not in self.cfg["databases"]:
                continue
            by_db.setdefault(db, {}).setdefault((schema, table), []).append(ch_name)

        all_counts: dict[str, int] = {}
        all_errors: dict[str, str] = {}
        if not by_db:
            return all_counts, all_errors

        with ThreadPoolExecutor(max_workers=min(SQL_DB_WORKERS, len(by_db))) as pool:
            futures = {
                pool.submit(self._fetch_one_db, db, wanted): db
                for db, wanted in by_db.items()
            }
            for fut in as_completed(futures):
                db = futures[fut]
                wanted = by_db[db]
                ch_names = [name for names in wanted.values() for name in names]
                try:
                    rows, err = fut.result()
                except Exception as exc:
                    err = _exc_text(exc)
                    rows = {}
                    logger.error(f"SQL fetch failed for {db}: {err}")
                if err:
                    for ch_name in ch_names:
                        all_errors[ch_name] = err
                        logger.error("SQL fetch failed for %s: %s", ch_name, err)
                else:
                    all_counts.update(rows)
                    logger.info(f"SQL fetched: {db} → {len(rows)} matched tables")

        return all_counts, all_errors

# ======================================================
# Comparator
# ======================================================
class TableComparator:
    def __init__(
        self,
        ch: dict[str, int],
        sql: dict[str, int],
        size_tiers: list[dict],
    ):
        self.ch = ch
        self.sql = sql
        self.size_tiers = size_tiers

    def compare(self) -> list[dict]:
        """
        Compare only tables that exist in both CH and SQL.
        Size tier / lag_limit is derived from the SQL row count at runtime.
        """
        mismatches = []

        for table, ch_count in self.ch.items():
            if table not in self.sql:
                continue
            sql_count = self.sql[table]
            diff = sql_count - ch_count
            if diff == 0:
                continue
            size_name, lag_limit = SizeThresholdConfig.classify(sql_count, self.size_tiers)
            mismatches.append({
                "table": table,
                "sql": sql_count,
                "ch": ch_count,
                "diff": diff,
                "size": size_name,
                "lag_limit": lag_limit,
            })

        return mismatches

# ======================================================
# Google Chat Alert (one combined message — all environments)
# ======================================================
GOOGLE_CHAT_MAX_BYTES = 32000

PRIORITY_SQL_TABLES = [
    ("GoGreen", "dbo", "CNTrack"),
    ("GoGreen", "dbo", "Booking"),
    ("GoGreen", "dbo", "ShipmentCollection"),
    ("GoGreen", "dbo", "Couriersheet"),
]
PRIORITY_SQL_KEYS = {
    (db.lower(), schema.lower(), table.lower())
    for db, schema, table in PRIORITY_SQL_TABLES
}


def _sql_key_from_ch(ch_name: str) -> tuple[str, str, str] | None:
    parsed = parse_ch_table_name(ch_name)
    if not parsed:
        return None
    db, schema, table = parsed
    return (db.lower(), schema.lower(), table.lower())


def _is_priority(ch_name: str) -> bool:
    key = _sql_key_from_ch(ch_name)
    return key is not None and key in PRIORITY_SQL_KEYS


def _exceeded(m: dict) -> bool:
    return abs(m["diff"]) > m["lag_limit"]


def _find_priority_row(mismatches: list[dict], sql_key: tuple[str, str, str]) -> dict | None:
    matches = [m for m in mismatches if _sql_key_from_ch(m["table"]) == sql_key]
    if not matches:
        return None
    matches.sort(key=lambda m: (not _exceeded(m), -abs(m["diff"])))
    return matches[0]


def _other_exceeded_by_env(env_results: dict) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for env, mismatches in env_results.items():
        rows = [
            m for m in mismatches
            if _exceeded(m) and not _is_priority(m["table"])
        ]
        rows.sort(key=lambda x: -abs(x["diff"]))
        out[env] = rows
    return out


def _fmt_metrics(m: dict, compact: bool) -> str:
    if compact:
        return (
            f"SQL {m['sql']:,} | CH {m['ch']:,} | Lag {m['diff']:,} | "
            f"{m['size']} | Thr {m['lag_limit']:,}"
        )
    return (
        f"SQL: {m['sql']} | CH: {m['ch']} | Lag: {m['diff']} | "
        f"Size: {m['size']} | Threshold: {m['lag_limit']}"
    )


def _has_fetch_errors(fetch_errors: dict[str, dict[str, str]] | None) -> bool:
    if not fetch_errors:
        return False
    return any(table_errors for table_errors in fetch_errors.values())


def _build_alert_text(
    env_results: dict,
    now: str,
    compact: bool,
    other_by_env: dict[str, list[dict]] | None = None,
    fetch_errors: dict[str, dict[str, str]] | None = None,
) -> str:
    if other_by_env is None:
        other_by_env = _other_exceeded_by_env(env_results)

    parts = [
        "📊 *SQL Server ↔ ClickHouse Lag Alert (V2)*",
        f"🕒 Time: {now}",
        "",
    ]

    if _has_fetch_errors(fetch_errors):
        parts.append("*FETCH ERRORS*")
        for env, table_errors in fetch_errors.items():
            if not table_errors:
                continue
            parts.append(f"*{env}*")
            for table, err in sorted(table_errors.items()):
                parts.append(f"  `{table}` — {err}")
            parts.append("")

    priority_blocks: list[list[str]] = []
    for db, schema, table in PRIORITY_SQL_TABLES:
        sql_key = (db.lower(), schema.lower(), table.lower())
        env_lines = []
        for env, mismatches in env_results.items():
            row = _find_priority_row(mismatches, sql_key)
            if row is not None and _exceeded(row):
                env_lines.append(f"  {env} — 🚨 {_fmt_metrics(row, compact)}")
        if env_lines:
            priority_blocks.append([f"*⭐ {db}.{schema}.{table}*", *env_lines, ""])

    if priority_blocks:
        parts.append("⭐ *PRIORITY TABLES*")
        for block in priority_blocks:
            parts.extend(block)

    exceeded_other = {env: rows for env, rows in other_by_env.items() if rows}
    if exceeded_other:
        parts.append("📋 *OTHER TABLES*")
        for env, rows in exceeded_other.items():
            parts.append(f"*{env}*")
            for m in rows:
                parts.append(f"🚨 `{m['table']}` | {_fmt_metrics(m, compact)}")
            parts.append("")

    return "\n".join(parts).rstrip() + "\n"


def _format_combined_alert(
    env_results: dict,
    now: str,
    fetch_errors: dict[str, dict[str, str]] | None = None,
) -> str | None:
    any_exceeded = any(
        _exceeded(m)
        for mismatches in env_results.values()
        for m in mismatches
    )
    if not any_exceeded and not _has_fetch_errors(fetch_errors):
        return None

    text = _build_alert_text(
        env_results, now, compact=False, fetch_errors=fetch_errors
    )
    if len(text.encode("utf-8")) <= GOOGLE_CHAT_MAX_BYTES:
        return text

    logger.warning(
        "Google Chat payload exceeds %d bytes; retrying with compact formatting.",
        GOOGLE_CHAT_MAX_BYTES,
    )
    text = _build_alert_text(
        env_results, now, compact=True, fetch_errors=fetch_errors
    )
    if len(text.encode("utf-8")) <= GOOGLE_CHAT_MAX_BYTES:
        return text

    trimmed = {env: list(rows) for env, rows in _other_exceeded_by_env(env_results).items()}
    dropped = 0
    while True:
        text = _build_alert_text(
            env_results,
            now,
            compact=True,
            other_by_env=trimmed,
            fetch_errors=fetch_errors,
        )
        if len(text.encode("utf-8")) <= GOOGLE_CHAT_MAX_BYTES:
            break
        dropped_one = False
        for env in reversed(list(env_results.keys())):
            if trimmed.get(env):
                trimmed[env].pop()
                dropped += 1
                dropped_one = True
                break
        if not dropped_one:
            break

    if dropped:
        logger.warning(
            "Dropped %d lowest-lag other-table row(s) to fit Google Chat %d-byte limit.",
            dropped,
            GOOGLE_CHAT_MAX_BYTES,
        )
    return text


def send_google_chat_alert(
    env_results: dict,
    fetch_errors: dict[str, dict[str, str]] | None = None,
):
    if not WEBHOOK_URL:
        logger.warning("⚠️  GOOGLE_CHAT_WEBHOOK_V2_URL is empty — alert NOT sent.")
        return

    now = datetime.now(PKT).strftime("%Y-%m-%d %I:%M:%S %p PKT")
    message_text = _format_combined_alert(env_results, now, fetch_errors)
    if not message_text:
        logger.info("All environments within thresholds. Google Chat alert skipped.")
        return

    logger.info("=" * 60)
    logger.info("📤 GOOGLE CHAT MESSAGE PREVIEW:")
    logger.info("=" * 60)
    for line in message_text.splitlines():
        logger.info(line)
    logger.info("=" * 60)

    requests.post(WEBHOOK_URL, json={"text": message_text}, timeout=30)
    logger.info("✅ Google Chat alert sent (all environments, one message).")


# ======================================================
# Per-environment / SQL workers
# ======================================================
def _prepare_ch_env(
    env_name: str,
    env_cfg: dict,
) -> tuple[str, ClickHouseClient, list[tuple[str, str, bool]], list[str], dict[str, str]]:
    logger.info(f"▶ Listing CH tables: {env_name}")
    config = Config(env_cfg["host"], env_cfg["port"])
    client = ClickHouseClient(config)
    jobs, names, errors = client.prepare_tables(label=env_name)
    logger.info(
        f"✔ CH listed: {env_name} — {len(names)} table(s), {len(jobs)} count job(s)"
    )
    return env_name, client, jobs, names, errors


def _count_ch_env(
    env_name: str,
    client: ClickHouseClient,
    jobs: list[tuple[str, str, bool]],
) -> tuple[str, dict[str, int], dict[str, str]]:
    logger.info(f"▶ Counting CH tables: {env_name}")
    counts, errors = client.count_prepared(jobs, label=env_name)
    logger.info(
        f"✔ CH counted: {env_name} — {len(counts)} table(s), {len(errors)} error(s)"
    )
    return env_name, counts, errors


def _fetch_sql(ch_union: set[str]) -> tuple[dict[str, int], dict[str, str]]:
    logger.info("▶ Starting SQL fetch (overlaps CH counts)…")
    _any_env = next(iter(CLICKHOUSE_ENVIRONMENTS.values()))
    sql_config = Config(_any_env["host"], _any_env["port"])
    counts, errors = SQLServerClient(sql_config).fetch_for_ch_tables(ch_union)
    logger.info(f"✔ SQL fetch complete — {len(counts)} matched table(s)")
    return counts, errors


# ======================================================
# App
# ======================================================
class App:
    def run(self):
        size_tiers = SizeThresholdConfig.load()
        logger.info(
            "Loaded size thresholds: "
            + ", ".join(
                f"{t['name']}(<{t['max_rows'] or '∞'}→{t['lag_limit']})"
                for t in size_tiers
            )
        )

        # ── STEP 1: List L2 tables on all 3 envs (parallel, metadata only) ──
        logger.info("━" * 60)
        logger.info("STEP 1 — Listing ClickHouse *_l2 / *_l2_vN tables (3 envs simultaneously)…")
        logger.info("━" * 60)

        env_ch: dict[str, dict[str, int]] = {name: {} for name in CLICKHOUSE_ENVIRONMENTS}
        fetch_errors: dict[str, dict[str, str]] = {}
        prepared: dict[str, tuple[ClickHouseClient, list[tuple[str, str, bool]]]] = {}
        ch_union: set[str] = set()

        with ThreadPoolExecutor(max_workers=CH_ENV_WORKERS) as pool:
            futures = {
                pool.submit(_prepare_ch_env, env_name, env_cfg): env_name
                for env_name, env_cfg in CLICKHOUSE_ENVIRONMENTS.items()
            }
            for fut in as_completed(futures):
                env_name = futures[fut]
                try:
                    name, client, jobs, names, errors = fut.result()
                    ch_union.update(names)
                    if errors:
                        fetch_errors.setdefault(name, {}).update(errors)
                    if jobs:
                        prepared[name] = (client, jobs)
                except Exception as exc:
                    err = _exc_text(exc)
                    logger.error(f"Environment list failed [{env_name}]: {err}")
                    fetch_errors[env_name] = {"(environment)": err}

        logger.info(f"ClickHouse union — {len(ch_union)} unique L2 table(s).")

        # ── STEP 2: Count all 3 envs + SQL fetch at the same time ───────────
        logger.info("━" * 60)
        logger.info("STEP 2 — Counting ClickHouse tables and fetching SQL (in parallel)…")
        logger.info("━" * 60)

        sql_counts: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=CH_ENV_WORKERS + 1) as pool:
            futures = {
                pool.submit(_count_ch_env, env_name, client, jobs): ("ch", env_name)
                for env_name, (client, jobs) in prepared.items()
            }
            futures[pool.submit(_fetch_sql, ch_union)] = ("sql", None)

            for fut in as_completed(futures):
                kind, env_name = futures[fut]
                try:
                    if kind == "ch":
                        name, counts, errors = fut.result()
                        env_ch[name] = counts
                        if errors:
                            fetch_errors.setdefault(name, {}).update(errors)
                    else:
                        sql_counts, sql_errors = fut.result()
                        if sql_errors:
                            fetch_errors["SQL Server"] = sql_errors
                except Exception as exc:
                    err = _exc_text(exc)
                    if kind == "ch":
                        logger.error(f"Environment count failed [{env_name}]: {err}")
                        fetch_errors.setdefault(env_name, {})["(environment)"] = err
                    else:
                        logger.error(f"SQL fetch failed: {err}")
                        fetch_errors["SQL Server"] = {"(sql)": err}

        # ── STEP 3: Compare + write CSVs ─────────────────────────────────────
        logger.info("━" * 60)
        logger.info("STEP 3 — Comparing and writing reports…")
        logger.info("━" * 60)

        env_results: dict[str, list[dict]] = {}
        for env_name in CLICKHOUSE_ENVIRONMENTS:
            ch_tables = env_ch.get(env_name, {})
            mismatches = TableComparator(ch_tables, sql_counts, size_tiers).compare()
            env_results[env_name] = mismatches
            logger.info(f"✔ {env_name} — {len(mismatches)} mismatch(es)")

        # ── STEP 4: Alert ────────────────────────────────────────────────────
        logger.info("━" * 60)
        logger.info("STEP 4 — Sending Google Chat alert (if needed)…")
        logger.info("━" * 60)

        ordered_results = {
            k: env_results[k]
            for k in CLICKHOUSE_ENVIRONMENTS
            if k in env_results
        }
        send_google_chat_alert(ordered_results, fetch_errors)

        logger.info("🎯 COMPARISON COMPLETED")


# ======================================================
# Main
# ======================================================
if __name__ == "__main__":
    App().run()
