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
        "host": "ch-prod-green.callcourier.com.pk--",
        "port": 443,
    },
    "🔵 Blue Environment": {
        "host": "ch-new.callcourier.com.pk",
        "port": 443,
    },
    "🟡 Yellow Environment": {
        "host": "ch-yellow.callcourier.com.pk--",
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
def _make_ch_client(cfg: dict):
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
        send_receive_timeout=300,
    )


class ClickHouseClient:
    def __init__(self, config):
        self.cfg = config.clickhouse
        self._meta_client = _make_ch_client(self.cfg)

    def _get_table_engines(self) -> dict[str, str]:
        """Returns {table_name_lower: engine_name}."""
        rows = self._meta_client.query(
            "SELECT name, engine FROM system.tables WHERE database = %(db)s",
            parameters={"db": self.cfg["database"]},
        ).result_rows
        return {name.lower(): engine for name, engine in rows}

    def _get_count_columns(self, table_names: list[str]) -> dict[str, str]:
        """
        Pick one countable column per table from system.columns.
        Prefer the first non-__deleted column; fall back to first column.
        """
        if not table_names:
            return {}

        rows = self._meta_client.query(
            """
            SELECT table, name, position
            FROM system.columns
            WHERE database = %(db)s AND table IN %(tables)s
            ORDER BY table, position
            """,
            parameters={"db": self.cfg["database"], "tables": tuple(table_names)},
        ).result_rows

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
    ) -> tuple[str, int]:
        """
        Count rows using a single column: count(col).
        Each thread uses its own client.
        """
        client = _make_ch_client(self.cfg)
        db = self.cfg["database"]
        try:
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
            cnt = client.query(query).result_rows[0][0]
        except Exception:
            cnt = 0
        finally:
            try:
                client.close()
            except Exception:
                pass
        return name, int(cnt)

    def list_l2_tables(self) -> list[tuple[str, bool]]:
        """Return [(table_name, use_final), ...] for *_l2 / *_l2_vN tables only."""
        engines = self._get_table_engines()
        rows = self._meta_client.query(
            "SELECT name FROM system.tables WHERE database = %(db)s",
            parameters={"db": self.cfg["database"]},
        ).result_rows

        work = []
        for (name,) in rows:
            t = name.lower()
            if t.endswith(("_kafka", "_mv")) or t.startswith("vw_") or not is_l2_table(t):
                continue
            engine = engines.get(t, "")
            use_final = engine.lower() == "replacingmergetree"
            work.append((name, use_final))
        return work

    def fetch_tables(self, label: str | None = None) -> dict[str, int]:
        work = self.list_l2_tables()
        if not work:
            return {}

        progress_label = label or f"CH [{self.cfg['host'].split('.')[0]}]"
        names = [name for name, _ in work]
        count_cols = self._get_count_columns(names)

        data: dict[str, int] = {}
        with ThreadPoolExecutor(max_workers=CH_TABLE_WORKERS) as pool:
            futures = {}
            for name, use_final in work:
                col = count_cols.get(name)
                if not col:
                    data[name] = 0
                    continue
                futures[pool.submit(self._count_one_table, name, col, use_final)] = name

            done = 0
            total = len(futures)
            logger.info("%s — counting %d table(s)…", progress_label, total)
            for fut in as_completed(futures):
                t_name, cnt = fut.result()
                data[t_name] = cnt
                done += 1
                log_table_progress(progress_label, done, total)

        return data

# ======================================================
# SQL Server Client (only tables present in ClickHouse)
# ======================================================
class SQLServerClient:
    def __init__(self, config):
        self.cfg = config.mssql

    def _connect(self, db: str):
        return pyodbc.connect(
            f"DRIVER={{{self.cfg['driver']}}};"
            f"SERVER={self.cfg['server']};DATABASE={db};"
            f"UID={self.cfg['username']};PWD={self.cfg['password']};"
            f"Encrypt=yes;TrustServerCertificate=yes;"
        )

    def _fetch_one_db(
        self, db: str, wanted: dict[tuple[str, str], list[str]]
    ) -> dict[str, int]:
        """
        Fetch row counts for specific (schema, table) pairs in one MSSQL database.
        wanted maps (schema, table) → list of ClickHouse table names to key the result.
        Returns {CH_table_name: sql_count}.
        """
        if not wanted:
            return {}

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

        conn = self._connect(db)
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

    def fetch_for_ch_tables(self, ch_table_names: set[str]) -> dict[str, int]:
        """
        Resolve CH names to SQL db/schema/table and fetch only those counts.
        Returns {CH_table_name: sql_count} keyed by the original CH names
        (so GG_dbo_CNTrack_l2_v1 keeps its versioned key).
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
        if not by_db:
            return all_counts

        with ThreadPoolExecutor(max_workers=min(SQL_DB_WORKERS, len(by_db))) as pool:
            futures = {
                pool.submit(self._fetch_one_db, db, wanted): db
                for db, wanted in by_db.items()
            }
            for fut in as_completed(futures):
                db = futures[fut]
                try:
                    rows = fut.result()
                    all_counts.update(rows)
                    logger.info(f"SQL fetched: {db} → {len(rows)} matched tables")
                except Exception as exc:
                    logger.error(f"SQL fetch failed for {db}: {exc}")

        return all_counts

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
# Google Chat Alert (one message per environment — avoids truncation)
# ======================================================
MAX_ALERT_ROWS = 25


def _format_env_alert(env: str, mismatches: list[dict], now: str) -> str | None:
    lines = []
    for m in sorted(mismatches, key=lambda x: -abs(x["diff"])):
        lag = abs(m["diff"])
        if lag <= m["lag_limit"]:
            continue
        lines.append(
            f"🚨 `{m['table']}` | "
            f"SQL: {m['sql']} | "
            f"CH: {m['ch']} | "
            f"Lag: {m['diff']} | "
            f"Size: {m['size']} | "
            f"Threshold: {m['lag_limit']}"
        )

    if not lines:
        return None

    extra = ""
    if len(lines) > MAX_ALERT_ROWS:
        extra = f"\n… and {len(lines) - MAX_ALERT_ROWS} more table(s)"
        lines = lines[:MAX_ALERT_ROWS]

    return (
        f"📊 *SQL Server ↔ ClickHouse Lag Alert (V2)*\n"
        f"🕒 Time: {now}\n"
        f"*{env}*\n\n"
        + "\n".join(lines)
        + extra
    )


def send_google_chat_alert(env_results: dict):
    if not WEBHOOK_URL:
        logger.warning("⚠️  GOOGLE_CHAT_WEBHOOK_V2_URL is empty — alert NOT sent.")
        return

    now = datetime.now(PKT).strftime("%Y-%m-%d %I:%M:%S %p PKT")
    sent = 0

    for env, mismatches in env_results.items():
        message_text = _format_env_alert(env, mismatches, now)
        if not message_text:
            logger.info("%s — no tables exceeded thresholds", env)
            continue

        logger.info("=" * 60)
        logger.info("📤 GOOGLE CHAT — %s", env)
        logger.info("=" * 60)
        for line in message_text.splitlines():
            logger.info(line)
        logger.info("=" * 60)

        requests.post(WEBHOOK_URL, json={"text": message_text}, timeout=30)
        sent += 1
        logger.info("✅ Google Chat alert sent for %s", env)

    if sent == 0:
        logger.info("All environments within thresholds. Google Chat alert skipped.")


# ======================================================
# Per-environment CH worker
# ======================================================
def _fetch_ch_env(
    env_name: str,
    env_cfg: dict,
) -> tuple[str, dict[str, int]]:
    logger.info(f"▶ Starting CH fetch: {env_name}")
    config = Config(env_cfg["host"], env_cfg["port"])
    ch_tables = ClickHouseClient(config).fetch_tables(label=env_name)
    logger.info(f"✔ CH done: {env_name} — {len(ch_tables)} table(s)")
    return env_name, ch_tables


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

        # ── STEP 1: ClickHouse first (all envs in parallel) ─────────────────
        logger.info("━" * 60)
        logger.info("STEP 1 — Fetching ClickHouse *_l2 / *_l2_vN tables (3 envs simultaneously)…")
        logger.info("━" * 60)

        env_ch: dict[str, dict[str, int]] = {}

        with ThreadPoolExecutor(max_workers=CH_ENV_WORKERS) as pool:
            futures = {
                pool.submit(_fetch_ch_env, env_name, env_cfg): env_name
                for env_name, env_cfg in CLICKHOUSE_ENVIRONMENTS.items()
            }
            for fut in as_completed(futures):
                env_name = futures[fut]
                try:
                    name, ch_tables = fut.result()
                    env_ch[name] = ch_tables
                except Exception as exc:
                    logger.error(f"Environment failed [{env_name}]: {exc}")
                    env_ch[env_name] = {}

        ch_union = set()
        for tables in env_ch.values():
            ch_union.update(tables.keys())

        logger.info(f"ClickHouse union — {len(ch_union)} unique L2 table(s).")

        # ── STEP 2: SQL only for tables found in ClickHouse ─────────────────
        logger.info("━" * 60)
        logger.info("STEP 2 — Fetching SQL Server counts for ClickHouse tables only…")
        logger.info("━" * 60)

        _any_env = next(iter(CLICKHOUSE_ENVIRONMENTS.values()))
        sql_config = Config(_any_env["host"], _any_env["port"])
        sql_counts = SQLServerClient(sql_config).fetch_for_ch_tables(ch_union)
        logger.info(f"SQL fetch complete — {len(sql_counts)} matched table(s).")

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
        send_google_chat_alert(ordered_results)

        logger.info("🎯 COMPARISON COMPLETED")


# ======================================================
# Main
# ======================================================
if __name__ == "__main__":
    App().run()
