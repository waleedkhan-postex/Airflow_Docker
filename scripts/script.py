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
    "🟢 Green Environment": {"host": "ch-prod-green.callcourier.com.pk", "port": 443},
    "🔵 Blue Environment":  {"host": "ch-new.callcourier.com.pk",        "port": 443},
    "🟡 Yellow Environment":{"host": "ch-yellow.callcourier.com.pk",     "port": 443},
}

# ======================================================
# Thread / Worker Settings
# ======================================================
CH_ENV_WORKERS   = len(CLICKHOUSE_ENVIRONMENTS)          # 3 envs in parallel
CH_TABLE_WORKERS = (os.cpu_count() or 4) * 2            # parallel row-count queries per env
SQL_DB_WORKERS   = 8                                     # parallel SQL DB connections

# ======================================================
# Global Settings
# ======================================================
os.environ["CLICKHOUSE_CONNECT_DISABLE_SSL_VERIFY"] = "1"
warnings.filterwarnings("ignore")
urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context

WEBHOOK_URL = (
    "https://chat.googleapis.com/v1/spaces/AAQApfkBULA/messages"
    "?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI"
    "&token=Nu0VBpFNvrb-xpzbVrwlyW9bpDaSxt5kRQQ_JgrrJ7c"
)
PKT = ZoneInfo("Asia/Karachi")

# ======================================================
# Logging
# ======================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")
logger = logging.getLogger("SQL_CH_COMPARE")

# ======================================================
# Config  (split into two focused dataclasses)
# ======================================================
DB_ABBREVIATION = {
    "HRM":          "HRM",
    "GoGreen":      "GG",
    "Cloud_GoGreen":"CGG",
    "SharedObject": "SO",
}

load_dotenv()  # load once at module level

def _ch_cfg(host: str, port: int) -> dict:
    return {
        "host":     host,
        "port":     port,
        "username": os.getenv("CLICKHOUSE_USER"),
        "password": os.getenv("CLICKHOUSE_PASSWORD"),
        "database": os.getenv("CLICKHOUSE_DATABASE"),
    }

def _sql_cfg() -> dict:
    return {
        "server":    os.getenv("MSSQL_SERVER"),
        "databases": os.getenv("MSSQL_DATABASES").split(","),
        "username":  os.getenv("MSSQL_USER"),
        "password":  os.getenv("MSSQL_PASSWORD"),
        "driver":    os.getenv("MSSQL_DRIVER"),
    }

# ======================================================
# Threshold Loader
# ======================================================
class ThresholdLoader:
    @staticmethod
    def load() -> dict[str, int]:
        df = pd.read_excel(THRESHOLD_FILE)
        df.columns = df.columns.astype(str).str.strip().str.lower()
        table_col     = next(c for c in df.columns if "table"     in c)
        threshold_col = next(c for c in df.columns if "threshold" in c)
        return {
            str(r[table_col]).strip().lower(): int(r[threshold_col])
            for _, r in df.iterrows()
        }

# ======================================================
# ClickHouse  (one fresh client per thread — library is not thread-safe)
# ======================================================
def _make_ch_client(cfg: dict):
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
    def __init__(self, cfg: dict):
        self.cfg = cfg

    # ── Optimisation 1: single metadata query instead of two ──────────────
    def _get_work_list(self) -> list[tuple[str, bool]]:
        """
        Returns [(table_name, use_final), …] in one round-trip.
        Previously this was two separate queries (table names + engines).
        """
        client = _make_ch_client(self.cfg)
        try:
            rows = client.query(
                """
                SELECT name, engine
                FROM system.tables
                WHERE database = %(db)s
                """,
                parameters={"db": self.cfg["database"]},
            ).result_rows
        finally:
            try:
                client.close()
            except Exception:
                pass

        work = []
        for name, engine in rows:
            t = name.lower()
            if t.endswith(("_kafka", "_mv")) or t.startswith("vw_"):
                continue
            use_final = engine.lower() == "replacingmergetree"
            work.append((name, use_final))
        return work

    def _count_one_table(self, name: str, use_final: bool) -> tuple[str, int]:
        """
        Each thread creates its own client.
        Root fix for CH returning 0 counts when a shared client is used.
        """
        client = _make_ch_client(self.cfg)
        try:
            db = self.cfg["database"]
            where = "FINAL WHERE __deleted='false'" if use_final else "WHERE __deleted='false'"
            cnt = client.query(
                f"SELECT count() FROM {db}.{name} {where}"
            ).result_rows[0][0]
        except Exception:
            cnt = 0
        finally:
            try:
                client.close()
            except Exception:
                pass
        return name.lower(), cnt

    def fetch_tables(self) -> dict[str, int]:
        work = self._get_work_list()
        data: dict[str, int] = {}

        with ThreadPoolExecutor(max_workers=CH_TABLE_WORKERS) as pool:
            futures = {
                pool.submit(self._count_one_table, name, use_final): name
                for name, use_final in work
            }
            label = self.cfg["host"].split(".")[0]
            with tqdm(total=len(futures), desc=f"CH [{label}]", leave=False) as pbar:
                for fut in as_completed(futures):
                    t_name, cnt = fut.result()
                    data[t_name] = cnt
                    pbar.update(1)

        return data

# ======================================================
# SQL Server  (connection string built once, reused per DB call)
# ======================================================
class SQLServerClient:
    _QUERY = """
        SELECT s.name schema_name, t.name table_name, SUM(p.rows) row_count
        FROM sys.tables t
        JOIN sys.schemas s      ON s.schema_id = t.schema_id
        JOIN sys.partitions p   ON p.object_id = t.object_id
        WHERE p.index_id IN (0, 1)
        GROUP BY s.name, t.name
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        # ── Optimisation 2: build the connection-string template once ──────
        self._conn_tmpl = (
            f"DRIVER={{{cfg['driver']}}};"
            f"SERVER={cfg['server']};"
            "DATABASE={db};"
            f"UID={cfg['username']};PWD={cfg['password']};"
            "Encrypt=yes;TrustServerCertificate=yes;"
        )

    def _fetch_one_db(self, db: str) -> list[dict]:
        conn = pyodbc.connect(self._conn_tmpl.format(db=db))
        try:
            df = pd.read_sql(self._QUERY, conn)
        finally:
            conn.close()

        return [
            {
                "database": db,
                "schema":   r["schema_name"],
                "table":    r["table_name"],
                "count":    int(r["row_count"]),
            }
            for _, r in df.iterrows()
        ]

    def fetch_tables(self) -> list[dict]:
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
# Comparator  (Optimisation 3: no longer needs a full Config object)
# ======================================================
class TableComparator:
    def __init__(self, sql: list[dict], ch: dict[str, int]):
        self.sql = sql
        self.ch  = ch

    def compare(self) -> list[dict]:
        mismatches = []
        for s in self.sql:
            abbr = DB_ABBREVIATION.get(s["database"])
            if not abbr:
                continue
            table = f"{abbr}_{s['schema']}_{s['table']}".lower()
            if table in self.ch:
                diff = s["count"] - self.ch[table]
                if diff != 0:
                    mismatches.append({
                        "table": table,
                        "sql":   s["count"],
                        "ch":    self.ch[table],
                        "diff":  diff,
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
            .replace("🔵", "").replace("🟢", "").replace("🟡", "")
            .strip()
        )
        df   = pd.DataFrame(mismatches).sort_values("diff", ascending=False)
        path = os.path.join(OUTPUT_DIR, f"table_comparison_{safe_env}.csv")
        df.to_csv(path, index=False)
        logger.info(f"CSV written: {path}")

# ======================================================
# Google Chat Alert
# (Optimisation 4: build message only when there is something to send)
# ======================================================
def send_google_chat_alert(env_results: dict[str, list[dict]], thresholds: dict[str, int]):
    now      = datetime.now(PKT).strftime("%Y-%m-%d %I:%M:%S %p PKT")
    sections = []
    alert_lines_exist = False

    for env, mismatches in env_results.items():
        lines = []
        for m in mismatches:
            lag = abs(m["diff"])
            if m["table"] in thresholds and lag > thresholds[m["table"]]:
                alert_lines_exist = True
                lines.append(
                    f"🚨 `{m['table']}` | "
                    f"SQL: {m['sql']} | CH: {m['ch']} | "
                    f"Lag: {m['diff']} | Threshold: {thresholds[m['table']]}"
                )
        sections.append(
            f"*{env}*\n" + ("\n".join(lines) if lines else "✅ No tables exceeded thresholds")
        )

    if not alert_lines_exist:
        logger.info("All environments within thresholds — Google Chat alert skipped.")
        return

    message_text = (
        f"📊 *SQL Server ↔ ClickHouse Lag Alert*\n"
        f"🕒 Time: {now}\n\n"
        + "\n\n".join(sections)
    )

    logger.info("=" * 60)
    logger.info("📤 GOOGLE CHAT MESSAGE PREVIEW:")
    logger.info("=" * 60)
    for line in message_text.splitlines():
        logger.info(line)
    logger.info("=" * 60)

    if WEBHOOK_URL:
        requests.post(WEBHOOK_URL, json={"text": message_text})
        logger.info("✅ Google Chat alert sent successfully.")
    else:
        logger.warning("⚠️  WEBHOOK_URL is empty — alert NOT sent.")

# ======================================================
# Per-environment worker (outer thread pool)
# ======================================================
def _run_env(
    env_name: str,
    env_cfg: dict,
    sql_tables: list[dict],
) -> tuple[str, list[dict]]:
    """
    Fetches CH counts for ONE environment, compares against the already-fetched
    SQL snapshot, writes the CSV, and returns (env_name, mismatches).
    Runs entirely in its own thread — no shared mutable state with other envs.
    """
    logger.info(f"▶ Starting CH fetch: {env_name}")
    cfg        = _ch_cfg(env_cfg["host"], env_cfg["port"])
    ch_tables  = ClickHouseClient(cfg).fetch_tables()
    mismatches = TableComparator(sql_tables, ch_tables).compare()
    ReportWriter.write(env_name, mismatches)
    logger.info(f"✔ Done: {env_name} — {len(mismatches)} mismatch(es)")
    return env_name, mismatches

# ======================================================
# App
# ======================================================
class App:
    def run(self):
        thresholds = ThresholdLoader.load()

        # ── STEP 1: Fetch SQL ONCE (all DBs in parallel) ─────────────────
        logger.info("━" * 60)
        logger.info("STEP 1 — Fetching SQL Server data (all databases in parallel)…")
        logger.info("━" * 60)
        sql_tables = SQLServerClient(_sql_cfg()).fetch_tables()
        logger.info(f"SQL fetch complete — {len(sql_tables)} total table records.")

        # ── STEP 2: All 3 CH environments IN PARALLEL ─────────────────────
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

        # ── STEP 3: Alert ─────────────────────────────────────────────────
        logger.info("━" * 60)
        logger.info("STEP 3 — Sending Google Chat alert (if needed)…")
        logger.info("━" * 60)

        # Restore Green → Blue → Yellow order regardless of thread-completion order
        ordered_results = {k: env_results[k] for k in CLICKHOUSE_ENVIRONMENTS if k in env_results}
        send_google_chat_alert(ordered_results, thresholds)

        logger.info("🎯 COMPARISON COMPLETED")

# ======================================================
# Main
# ======================================================
if __name__ == "__main__":
    App().run()