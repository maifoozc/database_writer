#!/usr/bin/env python3
"""
Standalone MQTT-to-DB ingest service for a private server.

Subscribes to an MQTT broker, routes messages by topic to PostgreSQL tables
(telemetry, alarms, alarm_history, locations), and runs until SIGTERM/SIGINT.

This script is independent: copy the folder to your server, set env (or .env),
install deps from requirements.txt, and run. No other project code required.

Topic → table: each topic pattern is mapped in TABLE_ROUTERS (add new tables there).
Config: MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_TOPIC, MQTT_USERNAME, MQTT_PASSWORD,
        MQTT_USE_TLS, DATABASE_URL, DB_BATCH_SIZE. Optional .env in this directory.

Run:  python mqtt_to_db.py   or   ./mqtt_to_db.py
"""

import json
import logging
import os
import signal
import sys
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

# Load .env from this script's directory so it works when run from anywhere
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(_SCRIPT_DIR, ".env"))
except ImportError:
    pass

logger = logging.getLogger("mqtt_to_db")

# ---------------------------------------------------------------------------
# Row mappers: payload dict -> tuple for INSERT (order matches table columns)
# ---------------------------------------------------------------------------

def _row_to_telemetry(row: Dict[str, Any]) -> Optional[Tuple]:
    """Meter data -> (time, site_id, source_id, register_name, device_id, value, response_time_ms).
    Accepts snake_case and camelCase (siteId, registerName, deviceId, responseTimeMs).
    device_id can be int or string (e.g. "1" or legacy "grid_meter" -> hashed to int).
    """
    try:
        t = row.get("time")
        sid = row.get("site_id") or row.get("siteId")
        src = row.get("id")
        reg = row.get("register_name") or row.get("registerName")
        did = row.get("device_id") or row.get("deviceId")
        val = row.get("value")
        rt = row.get("response_time_ms") or row.get("responseTimeMs")
        if t is None or sid is None or reg is None or did is None or val is None:
            return None
        # source_id: use id if present, else 0 (row still inserted)
        if src is None:
            src = 0
        time_str = t.isoformat() if hasattr(t, "isoformat") else str(t)
        # device_id: DB expects int; accept int or numeric string, or hash string (e.g. "grid_meter")
        try:
            device_id_int = int(did)
        except (TypeError, ValueError):
            device_id_int = abs(hash(str(did))) % (2**31)
        return (
            time_str,
            str(sid),
            int(src),
            str(reg),
            device_id_int,
            float(val),
            float(rt) if rt is not None else None,
        )
    except (TypeError, ValueError):
        return None


def _row_to_alarm(row: Dict[str, Any]) -> Optional[Tuple]:
    """Alarm payload -> (id, title, description, severity, category, status, timestamp, location, location_id, component, value, threshold)."""
    try:
        aid = row.get("id")
        if aid is None:
            return None
        ts = row.get("timestamp") or row.get("time")
        time_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts) if ts else None
        if not time_str:
            time_str = "now()"
        return (
            int(aid),
            str(row.get("title") or ""),
            str(row.get("description") or "") if row.get("description") is not None else None,
            str(row.get("severity") or "info"),
            str(row.get("category") or "system"),
            str(row.get("status") or "active"),
            time_str,
            str(row.get("location") or ""),
            str(row.get("location_id") or row.get("locationId") or "") if row.get("location_id") or row.get("locationId") else None,
            str(row.get("component")) if row.get("component") is not None else None,
            str(row.get("value")) if row.get("value") is not None else None,
            str(row.get("threshold")) if row.get("threshold") is not None else None,
        )
    except (TypeError, ValueError):
        return None


def _row_to_alarm_history(row: Dict[str, Any]) -> Optional[Tuple]:
    """Alarm history -> (id, alarm_id, state, action, user_id, user_role, timestamp, notes). id required for upsert."""
    try:
        hid = row.get("id")
        if hid is None:
            return None
        ts = row.get("timestamp") or row.get("time")
        time_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts) if ts else None
        if not time_str:
            return None
        return (
            int(hid),
            int(row["alarm_id"]),
            str(row.get("state") or ""),
            str(row.get("action")) if row.get("action") is not None else None,
            int(row["user_id"]) if row.get("user_id") is not None else None,
            str(row.get("user_role")) if row.get("user_role") is not None else None,
            time_str,
            str(row.get("notes")) if row.get("notes") is not None else None,
        )
    except (TypeError, ValueError, KeyError):
        return None


def _row_to_location(row: Dict[str, Any]) -> Optional[Tuple]:
    """Location -> (id, city, location, capacity, status)."""
    try:
        lid = row.get("id")
        if not lid:
            return None
        return (
            str(lid),
            str(row.get("city")) if row.get("city") is not None else None,
            str(row.get("location") or row.get("name") or ""),
            str(row.get("capacity")) if row.get("capacity") is not None else None,
            str(row.get("status") or "offline"),
        )
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Table router: topic pattern -> (table, columns, row_mapper, conflict_cols, update_cols)
# conflict_cols = tuple for ON CONFLICT; update_cols = tuple of columns to set on conflict
# ---------------------------------------------------------------------------

TelemetryRow = Tuple[Any, ...]
TableConfig = Tuple[str, Sequence[str], Callable[[Dict[str, Any]], Optional[TelemetryRow]], Sequence[str], Sequence[str]]

def _telemetry_columns() -> Sequence[str]:
    return ("time", "site_id", "source_id", "register_name", "device_id", "value", "response_time_ms")

def _alarm_columns() -> Sequence[str]:
    return ("id", "title", "description", "severity", "category", "status", "timestamp", "location", "location_id", "component", "value", "threshold")

def _alarm_history_columns() -> Sequence[str]:
    return ("id", "alarm_id", "state", "action", "user_id", "user_role", "timestamp", "notes")

def _location_columns() -> Sequence[str]:
    return ("id", "city", "location", "capacity", "status")

# Topic suffix or pattern -> (table_name, columns, row_mapper, conflict_columns, update_columns)
# conflict_columns empty => INSERT only (no ON CONFLICT)
TABLE_ROUTERS: List[Tuple[str, TableConfig]] = [
    ("meter_data", ("telemetry", _telemetry_columns(), _row_to_telemetry, (), ())),
    ("alarms", ("alarms", _alarm_columns(), _row_to_alarm, ("id",), ("title", "description", "severity", "category", "status", "timestamp", "location", "location_id", "component", "value", "threshold"))),
    ("alarm_history", ("alarm_history", _alarm_history_columns(), _row_to_alarm_history, ("id",), ("alarm_id", "state", "action", "user_id", "user_role", "timestamp", "notes"))),
    ("locations", ("locations", _location_columns(), _row_to_location, ("id",), ("city", "location", "capacity", "status"))),
]


def get_table_config_for_topic(topic: str) -> Optional[TableConfig]:
    """Resolve topic to (table, columns, row_mapper, conflict_cols, update_cols). Topic can be full path or suffix."""
    topic_normalized = topic.strip("/").split("/")[-1] if "/" in topic else topic
    for pattern, config in TABLE_ROUTERS:
        if topic_normalized == pattern or topic.endswith("/" + pattern):
            return config
    return None


# ---------------------------------------------------------------------------
# Payload parsing
# ---------------------------------------------------------------------------

def parse_payload(payload: Any) -> Optional[List[Dict[str, Any]]]:
    """Parse JSON from MQTT message. Returns list of row dicts (single object -> [obj])."""
    if isinstance(payload, bytes):
        try:
            data = json.loads(payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None
    elif isinstance(payload, (list, dict)):
        data = payload
    else:
        try:
            data = json.loads(str(payload))
        except json.JSONDecodeError:
            return None
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        # Wrapped payload: {"data": [...], "rows": [...], etc.}
        if "data" in data and isinstance(data["data"], list):
            return data["data"]
        if "rows" in data and isinstance(data["rows"], list):
            return data["rows"]
        return [data]
    return None


# ---------------------------------------------------------------------------
# Generic DB write (all tables)
# ---------------------------------------------------------------------------

def write_batch_to_db(
    connection_string: str,
    table: str,
    columns: Sequence[str],
    rows: List[Tuple],
    conflict_columns: Sequence[str],
    update_columns: Sequence[str],
) -> int:
    """Upsert a batch into any table. If conflict_columns, use ON CONFLICT ... DO UPDATE; else plain INSERT."""
    if not rows:
        return 0
    logger.info("Attempting to insert %s rows to table %s", len(rows), table)
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError:
        logger.error("psycopg2 not installed. pip install psycopg2-binary")
        return 0

    cols = ", ".join(columns)
    if conflict_columns and update_columns:
        conflict = ", ".join(conflict_columns)
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
        sql = f"""
        INSERT INTO {table} ({cols})
        VALUES %s
        ON CONFLICT ({conflict})
        DO UPDATE SET {updates}
        """
    else:
        sql = f"INSERT INTO {table} ({cols}) VALUES %s"
    try:
        conn = psycopg2.connect(connection_string)
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=min(len(rows), 500))
            conn.commit()
            return len(rows)
        finally:
            conn.close()
    except Exception as e:
        logger.error("DB write failed: %s", e, exc_info=True)
        return 0


def process_message(topic: str, payload: Any, connection_string: str) -> Dict[str, Any]:
    """
    Route one MQTT message to the correct table and write. Used by both MQTT loop and Lambda.
    Returns {"table": str, "written": int} or {"error": str}.
    """
    if not (connection_string or "").strip():
        logger.error("DATABASE_URL is empty; cannot write to database")
        return {"table": "", "written": 0}

    config = get_table_config_for_topic(topic)
    if not config:
        logger.info("No table config for topic %s (expected suffix: meter_data, alarms, alarm_history, or locations)", topic)
        return {"table": "", "written": 0}

    table, columns, row_mapper, conflict_cols, update_cols = config
    rows_data = parse_payload(payload)
    if not rows_data:
        logger.warning("Topic %s: payload could not be parsed as JSON list/object", topic)
        return {"table": table, "written": 0}

    rows: List[Tuple] = []
    for item in rows_data:
        row = row_mapper(item)
        if row is not None:
            rows.append(row)

    if not rows:
        # Log first row's keys and sample values so we can see why mapper rejected
        first = rows_data[0] if rows_data else {}
        logger.warning(
            "Topic %s -> %s: all %s row(s) were skipped (missing/invalid fields). "
            "First row keys: %s; required for telemetry: time, site_id, id, register_name, device_id, value",
            topic, table, len(rows_data), list(first.keys()),
        )
        return {"table": table, "written": 0}

    written = write_batch_to_db(connection_string, table, columns, rows, conflict_cols, update_cols)
    if written:
        logger.info("Attempting to write %s rows to table %s", written, table)
    return {"table": table, "written": written}


# ---------------------------------------------------------------------------
# Lambda handler (AWS Lambda entrypoint)
# ---------------------------------------------------------------------------

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda entrypoint. Event shape:
      - Direct: {"topic": "ems/site/1/meter_data", "payload": "[{...}]"}  (payload can be str or list)
      - IoT Core rule: event may have "topic", "payload" (or base64 decoded by rule)
    Set DATABASE_URL in Lambda environment.
    """
    logger.setLevel(logging.INFO)
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return {"statusCode": 500, "body": "DATABASE_URL not set"}

    topic = event.get("topic") or ""
    payload = event.get("payload")
    if payload is None and "data" in event:
        payload = event["data"]
    if payload is None:
        return {"statusCode": 400, "body": "Missing topic or payload"}

    # Optional: base64 decode if Lambda receives encoded payload
    if isinstance(payload, str) and payload.startswith("eyJ"):  # heuristic for base64 JSON
        try:
            import base64
            payload = base64.b64decode(payload).decode("utf-8")
        except Exception:
            pass

    result = process_message(topic, payload, db_url)
    written = result.get("written", 0)
    table = result.get("table", "")
    return {"statusCode": 200, "written": written, "table": table}


# ---------------------------------------------------------------------------
# Standalone MQTT subscriber (all topics from TABLE_ROUTERS)
# ---------------------------------------------------------------------------

DEFAULT_MQTT_HOST = "localhost"
DEFAULT_MQTT_PORT = 1883
DEFAULT_TOPIC = "ems/site/#"  # multi-level: matches ems/site/1/meter_data, etc.
DEFAULT_DB_BATCH_SIZE = 500


def get_config() -> Dict[str, Any]:
    return {
        "mqtt_host": os.environ.get("MQTT_BROKER_HOST", DEFAULT_MQTT_HOST),
        "mqtt_port": int(os.environ.get("MQTT_BROKER_PORT", str(DEFAULT_MQTT_PORT))),
        "mqtt_topic": os.environ.get("MQTT_TOPIC", DEFAULT_TOPIC),
        "mqtt_username": os.environ.get("MQTT_USERNAME"),
        "mqtt_password": os.environ.get("MQTT_PASSWORD"),
        "mqtt_use_tls": os.environ.get("MQTT_USE_TLS", "").strip().lower() in ("1", "true", "yes"),
        "database_url": os.environ.get("DATABASE_URL"),
        "db_batch_size": int(os.environ.get("DB_BATCH_SIZE", str(DEFAULT_DB_BATCH_SIZE))),
    }


def run_ingest(config: Dict[str, Any]) -> None:
    """Run MQTT client: subscribe to topic(s), on message route to table and write."""
    import paho.mqtt.client as mqtt

    db_url = config.get("database_url")
    if not db_url:
        logger.error("DATABASE_URL not set.")
        sys.exit(1)
    # Catch common mistake: .env.example uses "host" as placeholder
    if "@host:" in db_url or "@host/" in db_url:
        logger.error(
            'DATABASE_URL contains placeholder "host". '
            'Replace with your PostgreSQL server: localhost, 127.0.0.1, or server IP/hostname.'
        )
        sys.exit(1)

    topic = config["mqtt_topic"]

    def on_connect(client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any = None) -> None:
        if reason_code == 0:
            logger.info("Connected to MQTT %s:%s", config["mqtt_host"], config["mqtt_port"])
            client.subscribe(topic, qos=1)
            logger.info("Subscribed to %s", topic)
        else:
            logger.error("MQTT connect failed: %s", reason_code)

    def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        logger.info("Message received on %s (payload %s bytes)", msg.topic, len(msg.payload) if msg.payload else 0)
        result = process_message(msg.topic, msg.payload, db_url)
        w = result.get("written", 0)
        if w:
            logger.info("Topic %s -> %s: wrote %s rows", msg.topic, result.get("table", ""), w)
        else:
            # process_message already logs warnings for 0 rows; log at debug to avoid noise
            logger.debug("Topic %s -> written 0 rows (table=%s)", msg.topic, result.get("table", ""))

    def on_disconnect(client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any = None) -> None:
        if reason_code != 0:
            logger.warning("MQTT disconnected (rc=%s). Will reconnect.", reason_code)

    client_id = os.environ.get("MQTT_CLIENT_ID", "mqtt_to_db")
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
    )
    if config.get("mqtt_username"):
        client.username_pw_set(config["mqtt_username"], config.get("mqtt_password") or "")
    if config.get("mqtt_use_tls"):
        client.tls_set()

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    host, port = config["mqtt_host"], config["mqtt_port"]
    logger.info("Connecting to %s:%s, topic=%s", host, port, topic)
    client.connect(host, port, keepalive=60)

    def shutdown(signum: int, frame: Any) -> None:
        logger.info("Shutdown.")
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    client.loop_forever()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run_ingest(get_config())


if __name__ == "__main__":
    main()
