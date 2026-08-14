import hashlib
import json
import logging
import os
import ssl
import traceback
import warnings
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import clickhouse_connect
import requests
import urllib3
from dotenv import load_dotenv

# ======================================================
# Paths
# ======================================================
BASE_DIR = Path(__file__).resolve().parent
SCRIPTS_DIR = BASE_DIR.parent
STATE_FILE = BASE_DIR / "alerted_exceptions.json"

# Shared secrets for all scripts: scripts/.env
load_dotenv(SCRIPTS_DIR / ".env")

# ======================================================
# Global Settings
# ======================================================
os.environ["CLICKHOUSE_CONNECT_DISABLE_SSL_VERIFY"] = "1"
warnings.filterwarnings("ignore")
urllib3.disable_warnings()
ssl._create_default_https_context = ssl._create_unverified_context

PKT = ZoneInfo("Asia/Karachi")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    force=True,
)
logger = logging.getLogger("KAFKA_EXCEPTION_MONITOR")

# Sequential order: Green → Blue → Yellow (continue on success or failure)
CLICKHOUSE_ENVIRONMENTS = {
    "Green": {
        "host": "ch-prod-green.callcourier.com.pk",
        "port": 443,
    },
    "Blue": {
        "host": "ch-new.callcourier.com.pk",
        "port": 443,
    },
    "Yellow": {
        "host": "ch-yellow.callcourier.com.pk",
        "port": 443,
    },
}

# Flatten nested assignment arrays (ClickHouse Nested -> Array columns)
EXCEPTION_SQL = """
SELECT
    table,
    if(length(assignments.topic) > 0, arrayElement(assignments.topic, 1), '') AS topic,
    if(length(assignments.partition_id) > 0, arrayElement(assignments.partition_id, 1), -1) AS partition_id,
    if(length(assignments.current_offset) > 0, arrayElement(assignments.current_offset, 1), -1) AS current_offset,
    last_poll_time,
    last_commit_time,
    arrayMax(exceptions.time) AS last_exc_time,
    arrayElement(exceptions.text, -1) AS last_exception
FROM system.kafka_consumers
WHERE length(exceptions.text) > 0
ORDER BY last_exc_time DESC
"""


def _ch_config() -> dict[str, str]:
    """LOGISTICS_V2 credentials (CLICKHOUSE_V2_*); fall back to CLICKHOUSE_* if unset."""
    return {
        "username": os.getenv("CLICKHOUSE_V2_USER") or os.getenv("CLICKHOUSE_USER", ""),
        "password": os.getenv("CLICKHOUSE_V2_PASSWORD") or os.getenv("CLICKHOUSE_PASSWORD", ""),
        "database": (
            os.getenv("CLICKHOUSE_V2_DATABASE")
            or os.getenv("CLICKHOUSE_DATABASE")
            or "LOGISTICS_V2"
        ),
    }


def _mask(value: str, keep: int = 4) -> str:
    if not value:
        return "(empty)"
    if len(value) <= keep * 2:
        return "***"
    return f"{value[:keep]}...{value[-keep:]}"


def _short_exception(exc: str, max_chars: int = 500) -> str:
    """Keep first meaningful line(s); drop huge CH stack traces from alerts/logs."""
    if not exc:
        return ""
    lines = [ln.strip() for ln in str(exc).splitlines() if ln.strip()]
    if not lines:
        return ""
    # Prefer first non-frame line
    head = []
    for ln in lines:
        if ln[0:1].isdigit() and ". " in ln[:6]:
            break
        head.append(ln)
        if len(head) >= 3:
            break
    text = "\n".join(head) if head else lines[0]
    if len(text) > max_chars:
        return text[: max_chars - 3] + "..."
    return text


def _ch_client(host: str, port: int):
    cfg = _ch_config()
    logger.info(
        "Connecting to ClickHouse host=%s port=%s user=%s database=%s",
        host,
        port,
        cfg["username"],
        cfg["database"],
    )
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=cfg["username"],
        password=cfg["password"],
        database=cfg["database"],
        secure=True,
        verify=False,
    )


def load_state() -> set[str]:
    if STATE_FILE.exists():
        try:
            data = set(json.loads(STATE_FILE.read_text()))
            logger.info("Loaded dedupe state: %d fingerprint(s) from %s", len(data), STATE_FILE)
            return data
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning("Could not read state file %s (%s); starting fresh", STATE_FILE, exc)
    else:
        logger.info("No dedupe state file yet at %s", STATE_FILE)
    return set()


def save_state(keys: set[str]) -> None:
    trimmed = sorted(keys)[-5000:]
    STATE_FILE.write_text(json.dumps(trimmed, indent=2))
    logger.info("Saved dedupe state: %d fingerprint(s) -> %s", len(trimmed), STATE_FILE)


def fingerprint(env: str, row: dict) -> str:
    raw = (
        f"{env}|{row.get('table')}|{row.get('partition_id')}|"
        f"{row.get('current_offset')}|{_short_exception(row.get('last_exception') or '')}|"
        f"{row.get('last_exc_time')}"
    )
    return hashlib.sha1(raw.encode()).hexdigest()


def classify(exc: str) -> str:
    t = (exc or "").lower()
    if "cannot_commit_offset" in t or "commit attempts failed" in t:
        return "Commit failure"
    if any(
        x in t
        for x in (
            "cannot parse",
            "cannot_parse",
            "cannot convert",
            "type mismatch",
            "type_mismatch",
            "incorrect_data",
            "there is no column",
            "missing columns",
            "no_column",
            "null",
            "nullable",
            "out_of_range",
            "unexpected",
        )
    ):
        return "Schema/Parse"
    if any(
        x in t
        for x in (
            "timed out",
            "brokers are down",
            "apiversionrequest",
            "assignment lost",
            "unknown topic",
            "unknown topic or partition",
        )
    ):
        return "Broker/Rebalance"
    return "Other"


def fetch_exceptions(env: str, cfg: dict) -> list[dict]:
    logger.info("[%s] Querying system.kafka_consumers for exceptions...", env)
    client = _ch_client(cfg["host"], cfg["port"])
    try:
        result = client.query(EXCEPTION_SQL)
        cols = result.column_names
        rows = [dict(zip(cols, r)) for r in result.result_rows]
        for r in rows:
            r["environment"] = env
            r["last_exception"] = _short_exception(r.get("last_exception") or "")
        logger.info("[%s] Query OK — %d row(s) with exceptions", env, len(rows))
        for i, r in enumerate(rows, 1):
            logger.info(
                "[%s]   #%d table=%s topic=%s partition=%s offset=%s type=%s",
                env,
                i,
                r.get("table"),
                r.get("topic"),
                r.get("partition_id"),
                r.get("current_offset"),
                classify(r.get("last_exception") or ""),
            )
            logger.info("[%s]   #%d exception: %s", env, i, r.get("last_exception"))
        return rows
    finally:
        try:
            client.close()
            logger.info("[%s] ClickHouse connection closed", env)
        except Exception as exc:
            logger.warning("[%s] Failed to close ClickHouse client: %s", env, exc)


def format_exception_entry(row: dict, index: int) -> str:
    kind = classify(row.get("last_exception") or "")
    return (
        f"--- Exception #{index} ---\n"
        f"Type: {kind}\n"
        f"Table: {row.get('table')}\n"
        f"Topic: {row.get('topic')}\n"
        f"Partition: {row.get('partition_id')}\n"
        f"Offset: {row.get('current_offset')}\n"
        f"Last commit: {row.get('last_commit_time')}\n"
        f"Last exc time: {row.get('last_exc_time')}\n"
        f"Exception: {row.get('last_exception')}"
    )


def format_environment_digest(env: str, rows: list[dict]) -> str:
    """Single WhatsApp body listing every new exception for one environment."""
    now = datetime.now(PKT).strftime("%Y-%m-%d %I:%M:%S %p PKT")
    entries = [format_exception_entry(row, i + 1) for i, row in enumerate(rows)]
    return (
        f"ClickHouse Kafka Exception Digest\n"
        f"Environment: {env}\n"
        f"Time: {now}\n"
        f"New exceptions: {len(rows)}\n\n"
        + "\n\n".join(entries)
    )


def _build_lambda_msg(service_name: str, summary: str, *, alert_type: str = "down") -> str:
    """Uptime Kuma-style msg field required by the WhatsApp Lambda gateway."""
    status = "Up" if alert_type.lower() == "up" else "Down"
    return f"[{service_name}] [{status}] {summary}"


# Set True when gateway says group cannot be resolved — skip further sends this run
_WHATSAPP_DEST_BROKEN = False


def send_whatsapp(
    summary: str,
    *,
    environment: str,
    reason: str,
    service_name: str | None = None,
    monitor_url: str = "",
    alert_type: str = "down",
) -> bool:
    """Send via Lambda gateway. Requires x-api-key + destination-group headers and msg body."""
    global _WHATSAPP_DEST_BROKEN

    if _WHATSAPP_DEST_BROKEN:
        logger.error(
            "[%s] WhatsApp SKIPPED — destination group previously unresolved this run",
            environment,
        )
        return False

    url = os.getenv("WHATSAPP_API_URL", "").strip()
    api_key = os.getenv("WHATSAPP_API_KEY", "").strip()
    group_name = os.getenv("WHATSAPP_GROUP_NAME", "").strip()
    # Lambda lowercases destination-group and resolves via DESTINATION_GROUPS env on Lambda
    dest_group = group_name

    svc = service_name or f"ClickHouse Kafka - {environment}"
    msg = _build_lambda_msg(svc, summary, alert_type=alert_type)

    logger.info("=" * 60)
    logger.info("WHATSAPP SEND — Environment: %s | Reason: %s", environment, reason)
    logger.info("WHATSAPP SEND — destination-group: %s", dest_group or "(missing)")
    logger.info("WHATSAPP SEND — URL: %s", url or "(missing)")
    logger.info("WHATSAPP SEND — API key: %s", _mask(api_key))
    logger.info("WHATSAPP SEND — Lambda msg:\n%s", msg)
    logger.info("=" * 60)

    if not url or not api_key or not dest_group:
        logger.error(
            "[%s] WhatsApp NOT sent — missing env "
            "(WHATSAPP_API_URL / WHATSAPP_API_KEY / WHATSAPP_GROUP_NAME)",
            environment,
        )
        return False

    payload = {
        "msg": msg,
        "monitor": {"url": monitor_url or ""},
    }
    headers = {
        "x-api-key": api_key,
        "destination-group": dest_group,
        "Content-Type": "application/json",
    }

    try:
        logger.info(
            "[%s] POSTing WhatsApp alert (destination-group=%s)...",
            environment,
            dest_group,
        )
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        body = (resp.text or "")[:500]
        logger.info(
            "[%s] WhatsApp response status=%s body=%s",
            environment,
            resp.status_code,
            body,
        )
        if resp.status_code >= 400 and (
            "unresolved_groups" in body or "could be resolved" in body
        ):
            _WHATSAPP_DEST_BROKEN = True
            logger.error(
                "[%s] WhatsApp destination group NOT registered on gateway "
                "(destination-group=%r). Add a matching key to Lambda DESTINATION_GROUPS "
                "or fix WHATSAPP_GROUP_NAME in scripts/.env. "
                "Stopping further WhatsApp attempts this run.",
                environment,
                dest_group,
            )
            return False
        resp.raise_for_status()
        logger.info(
            "[%s] WhatsApp SENT OK → group=%s reason=%s",
            environment,
            dest_group,
            reason,
        )
        return True
    except requests.RequestException as exc:
        logger.error("[%s] WhatsApp SEND FAILED: %s", environment, exc)
        return False


def _process_environment(
    env: str,
    cfg: dict,
    seen: set[str],
    newly_alerted: set[str],
    summary: dict,
) -> None:
    """Fetch + alert for one environment. Never raises — caller always continues."""
    summary["checked"] += 1

    logger.info("-" * 60)
    logger.info("[%s] START check (SEQUENTIAL) host=%s port=%s", env, cfg["host"], cfg["port"])

    try:
        rows = fetch_exceptions(env, cfg)
    except Exception as exc:
        summary["env_failures"] += 1
        logger.error("[%s] FAILED to fetch exceptions: %s", env, exc)
        logger.error("[%s] Traceback:\n%s", env, traceback.format_exc())
        msg = (
            f"Exception monitor failed for {env}\n"
            f"Time: {datetime.now(PKT).strftime('%Y-%m-%d %I:%M:%S %p PKT')}\n"
            f"Host: {cfg['host']}\n"
            f"Error: {exc}"
        )
        ok = send_whatsapp(
            msg,
            environment=env,
            reason="monitor_fetch_failure",
            service_name=f"ClickHouse Kafka Monitor - {env}",
            monitor_url=f"https://{cfg['host']}",
        )
        if ok:
            summary["whatsapp_sent"] += 1
        else:
            summary["whatsapp_failed"] += 1
        logger.info("[%s] END check (FAILED) — continuing to next environment", env)
        return

    summary["exceptions_found"] += len(rows)

    logger.info("[%s] Consumers with exceptions: %d", env, len(rows))
    if not rows:
        logger.info("[%s] Healthy — no kafka consumer exceptions", env)

    new_rows: list[dict] = []
    new_keys: list[str] = []

    for row in rows:
        table = row.get("table")
        key = fingerprint(env, row)
        if key in seen or key in newly_alerted:
            summary["already_alerted_skipped"] += 1
            logger.info(
                "[%s] SKIP already-alerted table=%s partition=%s offset=%s",
                env,
                table,
                row.get("partition_id"),
                row.get("current_offset"),
            )
            continue

        new_rows.append(row)
        new_keys.append(key)
        logger.info(
            "[%s] NEW exception queued — table=%s type=%s",
            env,
            table,
            classify(row.get("last_exception") or ""),
        )

    if new_rows:
        digest = format_environment_digest(env, new_rows)
        logger.info(
            "[%s] Sending single WhatsApp digest with %d new exception(s)",
            env,
            len(new_rows),
        )
        ok = send_whatsapp(
            digest,
            environment=env,
            reason=f"kafka_exception_digest:{len(new_rows)}",
            service_name=f"ClickHouse Kafka - {env}",
            monitor_url=f"https://{cfg['host']}",
        )
        if ok:
            newly_alerted.update(new_keys)
            summary["whatsapp_sent"] += 1
        else:
            summary["whatsapp_failed"] += 1
            logger.error(
                "[%s] Digest NOT recorded in dedupe state because WhatsApp send failed "
                "(%d exception(s) will retry next run) — continuing",
                env,
                len(new_rows),
            )

    logger.info("[%s] END check (OK) — continuing to next environment", env)


def run() -> None:
    started = datetime.now(PKT)
    logger.info("#" * 60)
    logger.info("EXCEPTION MONITOR START — %s", started.strftime("%Y-%m-%d %I:%M:%S %p PKT"))
    logger.info("Mode: SEQUENTIAL — order Green → Blue → Yellow (continue on fail)")
    logger.info(
        "Environments to check: %s",
        ", ".join(f"{name} ({cfg['host']})" for name, cfg in CLICKHOUSE_ENVIRONMENTS.items()),
    )
    ch_cfg = _ch_config()
    logger.info(
        "ClickHouse V2 — user=%s database=%s",
        ch_cfg["username"] or "(missing)",
        ch_cfg["database"],
    )
    logger.info(
        "WhatsApp configured: url=%s group=%s key=%s",
        bool(os.getenv("WHATSAPP_API_URL", "").strip()),
        os.getenv("WHATSAPP_GROUP_NAME") or "(missing)",
        _mask(os.getenv("WHATSAPP_API_KEY", "")),
    )
    logger.info("#" * 60)

    seen = load_state()
    newly_alerted: set[str] = set()
    summary = {
        "checked": 0,
        "exceptions_found": 0,
        "already_alerted_skipped": 0,
        "whatsapp_sent": 0,
        "whatsapp_failed": 0,
        "env_failures": 0,
    }

    for env, cfg in CLICKHOUSE_ENVIRONMENTS.items():
        try:
            _process_environment(env, cfg, seen, newly_alerted, summary)
        except Exception as exc:
            # Hard safety net: never stop the sequence
            summary["env_failures"] += 1
            logger.error("[%s] Unhandled error (continuing): %s", env, exc)
            logger.error("[%s] Traceback:\n%s", env, traceback.format_exc())

    if newly_alerted:
        save_state(seen | newly_alerted)
    else:
        logger.info("No new successful alerts to persist in dedupe state")

    ended = datetime.now(PKT)
    logger.info("#" * 60)
    logger.info("EXCEPTION MONITOR SUMMARY")
    logger.info("  Started:              %s", started.strftime("%Y-%m-%d %I:%M:%S %p PKT"))
    logger.info("  Ended:                %s", ended.strftime("%Y-%m-%d %I:%M:%S %p PKT"))
    logger.info("  Environments checked: %d", summary["checked"])
    logger.info("  Exceptions found:     %d", summary["exceptions_found"])
    logger.info("  Skipped (deduped):    %d", summary["already_alerted_skipped"])
    logger.info("  WhatsApp sent:        %d", summary["whatsapp_sent"])
    logger.info("  WhatsApp failed:      %d", summary["whatsapp_failed"])
    logger.info("  Env fetch failures:   %d", summary["env_failures"])
    logger.info("#" * 60)

    if summary["whatsapp_failed"] or summary["env_failures"]:
        raise SystemExit(1)


if __name__ == "__main__":
    run()

