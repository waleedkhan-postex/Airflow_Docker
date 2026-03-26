import os
import ssl
import warnings
import logging
import urllib3
import pandas as pd
import pyodbc
import clickhouse_connect
from dotenv import load_dotenv
from tqdm import tqdm
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======================================================
# Paths
# ======================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_DIR = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output", "files")

os.makedirs(INPUT_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

THRESHOLD_FILE = os.path.join(INPUT_DIR, "threshold.xlsx")

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
# Outer pool: one thread per CH environment (all 3 run simultaneously)
CH_ENV_WORKERS = len(CLICKHOUSE_ENVIRONMENTS)

# Inner pool: parallel row-count queries WITHIN each CH environment.
# *** Each inner thread creates its OWN clickhouse client ***
# (clickhouse_connect is NOT thread-safe — sharing one client returns wrong/0 counts)
# Network-IO bound → 2× CPU cores is safe; raise if your CH can handle more.
CH_TABLE_WORKERS = (os.cpu_count() or 4) * 2

# SQL Server: parallel connections, one per configured database
SQL_DB_WORKERS = 8

# ======================================================
# Global Settings
# ======================================================
os.environ["CLICKHOUSE_CONNECT_DISABLE_SSL_VERIFY"] = "1"
warnings.filterwarnings("ignore")
urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context

WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAQApfkBULA/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=Nu0VBpFNvrb-xpzbVrwlyW9bpDaSxt5kRQQ_JgrrJ7c"
WEBHOOK_URL = ""
# Asia/Karachi Timezone
PKT = ZoneInfo("Asia/Karachi")

# ======================================================
# Logging
# ======================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("SQL_CH_COMPARE")

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

    def __init__(self, ch_host, ch_port):
        load_dotenv()
        self.clickhouse = {
            "host": ch_host,
            "port": ch_port,
            "username": os.getenv("CLICKHOUSE_USER"),
            "password": os.getenv("CLICKHOUSE_PASSWORD"),
            "database": os.getenv("CLICKHOUSE_DATABASE"),
        }
        self.mssql = {
            "server": os.getenv("MSSQL_SERVER"),
            "databases": os.getenv("MSSQL_DATABASES").split(","),
            "username": os.getenv("MSSQL_USER"),
            "password": os.getenv("MSSQL_PASSWORD"),
            "driver": os.getenv("MSSQL_DRIVER"),
        }

# ======================================================
# Threshold Loader
# ======================================================
class ThresholdLoader:
    @staticmethod
    def load():
        df = pd.read_excel(THRESHOLD_FILE)
        df.columns = df.columns.astype(str).str.strip().str.lower()

        table_col = next(c for c in df.columns if "table" in c)
        threshold_col = next(c for c in df.columns if "threshold" in c)

        return {
            str(r[table_col]).strip().lower(): int(r[threshold_col])
            for _, r in df.iterrows()
        }

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
        secure=False,
        verify=False,
    )


class ClickHouseClient:
    def __init__(self, config):
        self.cfg = config.clickhouse
        # Dedicated client used only for single-threaded metadata queries
        self._meta_client = _make_ch_client(self.cfg)

    def _get_table_engines(self) -> dict[str, str]:
        """Returns {table_name_lower: engine_name}."""
        rows = self._meta_client.query(
            "SELECT name, engine FROM system.tables WHERE database = %(db)s",
            parameters={"db": self.cfg["database"]},
        ).result_rows
        return {name.lower(): engine for name, engine in rows}

    def _count_one_table(self, name: str, use_final: bool) -> tuple[str, int]:
        """
        Query row count for ONE table.
        Creates its own client — never shares a connection with other threads.
        This is the root fix for ClickHouse returning 0 counts in the previous version.
        """
        client = _make_ch_client(self.cfg)
        try:
            if use_final:
                query = (
                    f"SELECT count() FROM {self.cfg['database']}.{name} "
                    f"FINAL WHERE __deleted='false'"
                )
            else:
                query = (
                    f"SELECT count() FROM {self.cfg['database']}.{name} "
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
        return name.lower(), cnt

    def fetch_tables(self) -> dict[str, int]:
        engines = self._get_table_engines()

        # Build work list — identical filtering logic to the original
        rows = self._meta_client.query(
            "SELECT name FROM system.tables WHERE database = %(db)s",
            parameters={"db": self.cfg["database"]},
        ).result_rows

        work = []
        for (name,) in rows:
            t = name.lower()
            if t.endswith(("_kafka", "_mv")) or t.startswith("vw_"):
                continue
            engine = engines.get(t, "")
            use_final = engine.lower() == "replacingmergetree"
            work.append((name, use_final))

        data: dict[str, int] = {}

        with ThreadPoolExecutor(max_workers=CH_TABLE_WORKERS) as pool:
            futures = {
                pool.submit(self._count_one_table, name, use_final): name
                for name, use_final in work
            }
            with tqdm(
                total=len(futures),
                desc=f"CH [{self.cfg['host'].split('.')[0]}]",
                leave=False,
            ) as pbar:
                for fut in as_completed(futures):
                    t_name, cnt = fut.result()
                    data[t_name] = cnt
                    pbar.update(1)

        return data

# ======================================================
# SQL Server Client
# ======================================================
class SQLServerClient:
    def __init__(self, config):
        self.cfg = config.mssql

    def _fetch_one_db(self, db: str) -> list[dict]:
        """
        Fetch row counts from one MSSQL database.
        Identical query to the original — no logic changes.
        """
        query = """
        SELECT s.name schema_name, t.name table_name, SUM(p.rows) row_count
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.partitions p ON p.object_id = t.object_id
        WHERE p.index_id IN (0,1)
        GROUP BY s.name, t.name
        """
        conn = pyodbc.connect(
            f"DRIVER={{{self.cfg['driver']}}};"
            f"SERVER={self.cfg['server']};DATABASE={db};"
            f"UID={self.cfg['username']};PWD={self.cfg['password']};"
            f"Encrypt=yes;TrustServerCertificate=yes;"
        )
        df = pd.read_sql(query, conn)
        conn.close()

        return [
            {
                "database": db,
                "schema": r["schema_name"],
                "table": r["table_name"],
                "count": int(r["row_count"]),
            }
            for _, r in df.iterrows()
        ]

    def fetch_tables(self) -> list[dict]:
        """
        Connects to all configured databases in parallel (one thread per DB).
        SQL is fetched ONCE and shared read-only across all CH environment threads.
        """
        databases = self.cfg["databases"]
        all_rows: list[dict] = []

        with ThreadPoolExecutor(max_workers=min(SQL_DB_WORKERS, len(databases))) as pool:
            futures = {pool.submit(self._fetch_one_db, db): db for db in databases}
            for fut in as_completed(futures):
                db = futures[fut]
                try:
                    rows = fut.result()
                    all_rows.extend(rows)
                    logger.info(f"SQL fetched: {db} → {len(rows)} tables")
                except Exception as exc:
                    logger.error(f"SQL fetch failed for {db}: {exc}")

        return all_rows

# ======================================================
# Comparator
# ======================================================
class TableComparator:
    def __init__(self, config, sql: list[dict], ch: dict[str, int]):
        self.cfg = config
        self.sql = sql
        self.ch = ch

    def compare(self) -> list[dict]:
        mismatches = []

        for s in self.sql:
            abbr = self.cfg.DB_ABBREVIATION.get(s["database"])
            if not abbr:
                continue

            table = f"{abbr}_{s['schema']}_{s['table']}".lower()
            if table in self.ch:
                diff = s["count"] - self.ch[table]
                if diff != 0:
                    mismatches.append({
                        "table": table,
                        "sql": s["count"],
                        "ch": self.ch[table],
                        "diff": diff,
                    })

        return mismatches

# ======================================================
# CSV Writer (per env)
# ======================================================
class ReportWriter:
    @staticmethod
    def write(env_name: str, mismatches: list[dict]):
        if not mismatches:
            return

        safe_env = (
            env_name.lower()
            .replace(" ", "_")
            .replace("🔵", "")
            .replace("🟢", "")
            .replace("🟡", "")
            .strip()
        )

        df = pd.DataFrame(mismatches).sort_values("diff", ascending=False)
        path = os.path.join(OUTPUT_DIR, f"table_comparison_{safe_env}.csv")
        df.to_csv(path, index=False)
        logger.info(f"CSV written: {path}")

# ======================================================
# Google Chat Alert
# ======================================================
def send_google_chat_alert(env_results: dict, thresholds: dict):
    now = datetime.now(PKT).strftime("%Y-%m-%d %I:%M:%S %p PKT")

    sections = []
    has_any_alert = False

    for env, mismatches in env_results.items():
        lines = []

        for m in mismatches:
            t = m["table"]
            lag = abs(m["diff"])

            if t in thresholds and lag > thresholds[t]:
                has_any_alert = True
                lines.append(
                    f"🚨 `{t}` | "
                    f"SQL: {m['sql']} | "
                    f"CH: {m['ch']} | "
                    f"Lag: {m['diff']} | "
                    f"Threshold: {thresholds[t]}"
                )

        if lines:
            sections.append(f"*{env}*\n" + "\n".join(lines))
        else:
            sections.append(f"*{env}*\n✅ No tables exceeded thresholds")

    if not has_any_alert:
        logger.info("All environments within thresholds. Google Chat alert skipped.")
        return

    message_text = (
        f"📊 *SQL Server ↔ ClickHouse Lag Alert*\n"
        f"🕒 Time: {now}\n\n"
        + "\n\n".join(sections)
    )

    payload = {"text": message_text}

    logger.info("=" * 60)
    logger.info("📤 GOOGLE CHAT MESSAGE PREVIEW:")
    logger.info("=" * 60)
    for line in message_text.splitlines():
        logger.info(line)
    logger.info("=" * 60)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json=payload)
        logger.info("✅ Google Chat alert sent successfully.")
    else:
        logger.warning("⚠️  WEBHOOK_URL is empty — alert NOT sent.")


# ======================================================
# Per-environment worker (called from outer thread pool)
# ======================================================
def _run_env(
    env_name: str,
    env_cfg: dict,
    sql_tables: list[dict],
) -> tuple[str, list[dict]]:
    """
    Fetches CH data for ONE environment, compares against the already-fetched
    SQL snapshot, writes the CSV, and returns (env_name, mismatches).
    Runs entirely in its own thread — no shared mutable state with other envs.
    """
    logger.info(f"▶ Starting CH fetch: {env_name}")
    config = Config(env_cfg["host"], env_cfg["port"])

    ch_tables = ClickHouseClient(config).fetch_tables()
    mismatches = TableComparator(config, sql_tables, ch_tables).compare()
    ReportWriter.write(env_name, mismatches)

    logger.info(f"✔ Done: {env_name} — {len(mismatches)} mismatch(es)")
    return env_name, mismatches


# ======================================================
# App
# ======================================================
class App:
    def run(self):
        thresholds = ThresholdLoader.load()

        # ── STEP 1: Fetch SQL ONCE ───────────────────────────────────────────
        # MSSQL credentials come entirely from .env — the CH host/port used
        # to construct Config here has no effect on the SQL connection.
        logger.info("━" * 60)
        logger.info("STEP 1 — Fetching SQL Server data (all databases in parallel)…")
        logger.info("━" * 60)

        _any_env = next(iter(CLICKHOUSE_ENVIRONMENTS.values()))
        sql_config = Config(_any_env["host"], _any_env["port"])
        sql_tables = SQLServerClient(sql_config).fetch_tables()

        logger.info(f"SQL fetch complete — {len(sql_tables)} total table records.")

        # ── STEP 2: All 3 CH environments IN PARALLEL ───────────────────────
        logger.info("━" * 60)
        logger.info("STEP 2 — Fetching ClickHouse counts (3 envs simultaneously)…")
        logger.info("━" * 60)

        env_results: dict[str, list[dict]] = {}

        with ThreadPoolExecutor(max_workers=CH_ENV_WORKERS) as pool:
            futures = {
                pool.submit(_run_env, env_name, env_cfg, sql_tables): env_name
                for env_name, env_cfg in CLICKHOUSE_ENVIRONMENTS.items()
            }
            for fut in as_completed(futures):
                env_name = futures[fut]
                try:
                    name, mismatches = fut.result()
                    env_results[name] = mismatches
                except Exception as exc:
                    logger.error(f"Environment failed [{env_name}]: {exc}")
                    env_results[env_name] = []

        # ── STEP 3: Alert ────────────────────────────────────────────────────
        logger.info("━" * 60)
        logger.info("STEP 3 — Sending Google Chat alert (if needed)…")
        logger.info("━" * 60)

        # Restore Green → Blue → Yellow order regardless of thread completion order
        ordered_results = {
            k: env_results[k]
            for k in CLICKHOUSE_ENVIRONMENTS
            if k in env_results
        }
        send_google_chat_alert(ordered_results, thresholds)

        logger.info("🎯 COMPARISON COMPLETED")


# ======================================================
# Main
# ======================================================
if __name__ == "__main__":
    App().run()