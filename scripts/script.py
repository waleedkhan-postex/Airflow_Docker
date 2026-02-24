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
import time

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
# ClickHouse Environments
# ======================================================
CLICKHOUSE_ENVIRONMENTS = {
    "🔵 Blue Environment": {
        "host": "ch-new.callcourier.com.pk",
        "port": 443,
    },
    "🟢 Green Environment": {
        "host": "ch-prod-green.callcourier.com.pk",
        "port": 443,
    },
    "🟡 Yellow Environment": {
        "host": "ch-yellow.callcourier.com.pk",
        "port": 443,
    },
}

# ======================================================
# Global Settings
# ======================================================
os.environ["CLICKHOUSE_CONNECT_DISABLE_SSL_VERIFY"] = "1"
warnings.filterwarnings("ignore")
urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context

WEBHOOK_URL = "https://chat.googleapis.com/v1/spaces/AAQApfkBULA/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=Nu0VBpFNvrb-xpzbVrwlyW9bpDaSxt5kRQQ_JgrrJ7c"

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
        "SharedObject" : "SO",
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
class ClickHouseClient:
    def __init__(self, config):
        self.cfg = config.clickhouse
        self.client = clickhouse_connect.get_client(
            **self.cfg, secure=False, verify=False
        )

    def fetch_tables(self):
        rows = self.client.query(
            "SELECT name FROM system.tables WHERE database = %(db)s",
            parameters={"db": self.cfg["database"]},
        ).result_rows

        data = {}
        for (name,) in tqdm(rows, desc=f"ClickHouse [{self.cfg['host']}]"):
            t = name.lower()
            if t.endswith(("_kafka", "_mv")) or t.startswith("vw_"):
                continue

            try:
                cnt = self.client.query(
                    f"SELECT count() FROM {self.cfg['database']}.{name} FINAL WHERE __deleted='false'"
                ).result_rows[0][0]
            except Exception:
                cnt = 0

            data[t] = cnt
        return data

# ======================================================
# SQL Server Client
# ======================================================
class SQLServerClient:
    def __init__(self, config):
        self.cfg = config.mssql

    def fetch_tables(self):
        query = """
        SELECT s.name schema_name, t.name table_name, SUM(p.rows) row_count
        FROM sys.tables t
        JOIN sys.schemas s ON s.schema_id = t.schema_id
        JOIN sys.partitions p ON p.object_id = t.object_id
        WHERE p.index_id IN (0,1)
        GROUP BY s.name, t.name
        """

        result = []
        for db in self.cfg["databases"]:
            conn = pyodbc.connect(
                f"DRIVER={{{self.cfg['driver']}}};"
                f"SERVER={self.cfg['server']};DATABASE={db};"
                f"UID={self.cfg['username']};PWD={self.cfg['password']};"
                f"Encrypt=yes;TrustServerCertificate=yes;"
            )
            df = pd.read_sql(query, conn)
            conn.close()

            for _, r in df.iterrows():
                result.append({
                    "database": db,
                    "schema": r["schema_name"],
                    "table": r["table_name"],
                    "count": int(r["row_count"]),
                })
        return result

# ======================================================
# Comparator
# ======================================================
class TableComparator:
    def __init__(self, config, sql, ch):
        self.cfg = config
        self.sql = sql
        self.ch = ch

    def compare(self):
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
# CSV Writer (PER ENV)
# ======================================================
class ReportWriter:
    @staticmethod
    def write(env_name, mismatches):
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
def send_google_chat_alert(env_results, thresholds):
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    sections = []
    has_any_alert = False   # 👈 track if ANY env has breach

    for env, mismatches in env_results.items():
        lines = []

        for m in mismatches:
            t = m["table"]
            lag = abs(m["diff"])

            if t in thresholds and lag > thresholds[t]:
                has_any_alert = True  # 👈 at least one breach found
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

    # 🚫 If BOTH environments are healthy → NO alert
    if not has_any_alert:
        logger.info("All environments are within thresholds. Google Chat alert skipped.")
        return

    payload = {
        "text": (
            f"📊 *SQL Server ↔ ClickHouse Lag Alert*\n"
            f"🕒 Time: {now}\n\n"
            + "\n\n".join(sections)
        )
    }

    requests.post(WEBHOOK_URL, json=payload)


# ======================================================
# App
# ======================================================
class App:
    def run(self):
        thresholds = ThresholdLoader.load()
        env_results = {}

        for env, cfg in CLICKHOUSE_ENVIRONMENTS.items():
            logger.info(f"Running for {env}")
            config = Config(cfg["host"], cfg["port"])

            ch_tables = ClickHouseClient(config).fetch_tables()
            sql_tables = SQLServerClient(config).fetch_tables()

            mismatches = TableComparator(config, sql_tables, ch_tables).compare()
            env_results[env] = mismatches

            ReportWriter.write(env, mismatches)

        send_google_chat_alert(env_results, thresholds)
        logger.info("🎯 COMPARISON COMPLETED")

# ======================================================
# Main
# ======================================================
if __name__ == "__main__":
    App().run()
