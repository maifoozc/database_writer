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
from datetime import datetime, timezone

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
        source_id = row.get("source_id") # Get source_id (local SQLite row ID)
        reg = row.get("register_name") or row.get("registerName")
        device_id_raw = row.get("device_id") or row.get("deviceId") # Get device_id (actual device identifier)
        val = row.get("value")
        rt = row.get("response_time_ms") or row.get("responseTimeMs")

        if t is None or sid is None or source_id is None or reg is None or device_id_raw is None or val is None:
            logger.warning(f"Skipping row in _row_to_telemetry due to missing required fields. Row: {row}")
            return None

        time_str = t.isoformat() if hasattr(t, "isoformat") else str(t)

        # device_id: DB expects int; accept int or numeric string, or hash string (e.g. "grid_meter")
        try:
            device_id_int = int(device_id_raw)
        except (TypeError, ValueError):
            device_id_int = abs(hash(str(device_id_raw))) % (2**31)

        return (
            time_str,
            str(sid),
            int(source_id), # source_id is BIGINT in DB
            str(reg),
            device_id_int, # device_id is INT in DB
            float(val),
            float(rt) if rt is not None else None,
        )
    except (TypeError, ValueError) as e:
        logger.warning(f"Skipping row in _row_to_telemetry due to data conversion error: {e}, row: {row}")
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
    ("meter_data", ("telemetry", _telemetry_columns(), _row_to_telemetry, ("time", "site_id", "source_id"), ())),
    ("alarms", ("alarms", _alarm_columns(), _row_to_alarm, ("id",), ("title", "description", "severity", "category", "status", "timestamp", "location", "location_id", "component", "value", "threshold"))),
    ("alarm_history", ("alarm_history", _alarm_history_columns(), _row_to_alarm_history, ("id",), ("alarm_id", "state", "action", "user_id", "user_role", "timestamp", "notes"))),
    ("locations", ("locations", _location_columns(), _row_to_location, ("id",), ("city", "location", "capacity", "status"))),
]


def get_table_config_for_topic(topic: str) -> Optional[TableConfig]:
    """Resolve topic to (table, columns, row_mapper, conflict_cols, update_cols). Topic can be full path or suffix."""
    topic_normalized = topic.strip("/").split("/")[-1] if "/" in topic else topic
    logger.debug(f"Normalized topic for lookup: '{topic_normalized}' (original: '{topic}')") # New debug log
    for pattern, config in TABLE_ROUTERS:
        logger.debug(f"Attempting to match '{topic_normalized}' with pattern '{pattern}' or topic ending with '/{pattern}'") # New debug log
        if topic_normalized == pattern or topic.endswith("/" + pattern):
            logger.debug(f"Match found for topic '{topic}' with pattern '{pattern}'. Table: '{config[0]}'") # New debug log
            return config
    logger.debug(f"No table config found for topic: '{topic}'") # New debug log
    return None


def _validate_table_schema(db_conn: Any, table_name: str, expected_columns: Sequence[str]) -> bool:
    """
    Validates if the given table in the database has the expected columns.
    Returns True if valid, False otherwise.
    """
    try:
        with db_conn.cursor() as cur:
            cur.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = current_schema() AND table_name = %s
                ORDER BY ordinal_position;
            """, (table_name,))
            actual_columns = [row[0] for row in cur.fetchall()]

        expected_set = set(expected_columns)
        actual_set = set(actual_columns)

        if not actual_set:
            logger.critical(f"❌ Schema Validation Failed: Table '{table_name}' does not exist.")
            return False

        missing_columns = expected_set - actual_set
        extra_columns = actual_set - expected_set

        if missing_columns:
            logger.critical(f"❌ Schema Validation Failed for table '{table_name}': Missing columns: {', '.join(missing_columns)}")
            return False
        if extra_columns:
            logger.warning(f"⚠️ Schema Validation Warning for table '{table_name}': Extra columns found: {', '.join(extra_columns)}")
            # This is a warning, not a critical failure, as extra columns usually don't break inserts

        logger.info(f"✅ Schema Validation Succeeded for table '{table_name}'.")
        return True

    except Exception as e:
        logger.critical(f"❌ Error during schema validation for table '{table_name}': {e}", exc_info=True)
        return False


def _write_to_dlq(dlq_file_path: str, dlq_max_size_mb: int, topic: str, payload: Any, reason: str) -> None:
    """Writes failed messages to a Dead Letter Queue file, managing its size."""
    try:
        # Convert max_size_mb to bytes
        max_size_bytes = dlq_max_size_mb * 1024 * 1024

        # Check file size before writing
        if os.path.exists(dlq_file_path) and os.path.getsize(dlq_file_path) >= max_size_bytes:
            logger.warning(f"DLQ file '{dlq_file_path}' reached max size ({dlq_max_size_mb} MB). Rotating/truncating.")
            # Simple rotation: truncate to half its size
            with open(dlq_file_path, 'r+') as f:
                f.seek(max_size_bytes // 2)
                remaining_content = f.read()
                f.seek(0)
                f.truncate()
                f.write(remaining_content)

        timestamp = datetime.now().isoformat()
        with open(dlq_file_path, 'a') as f:
            f.write(json.dumps({
                "timestamp": timestamp,
                "topic": topic,
                "reason": reason,
                "payload": payload if isinstance(payload, str) else str(payload) # Ensure payload is string for JSON dump
            }) + "\n")
        logger.error(f"Message for topic '{topic}' written to DLQ due to: {reason}")
    except Exception as e:
        logger.critical(f"❌ Failed to write to DLQ file '{dlq_file_path}': {e}", exc_info=True)


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
    db_conn: Any, # Changed from connection_string
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
    logger.debug("Sample data for table %s: %s", table, rows[:2])
    try:
        import psycopg2
        from psycopg2.extras import execute_values
    except ImportError:
        logger.error("psycopg2 not installed. pip install psycopg2-binary")
        return 0

    cols = ", ".join(columns)
    if conflict_columns:
        conflict = ", ".join(conflict_columns)
        if update_columns:
            updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
            sql = f"""
        INSERT INTO {table} ({cols})
        VALUES %s
        ON CONFLICT ({conflict})
        DO UPDATE SET {updates}
        """
        else:
            sql = f"""
        INSERT INTO {table} ({cols})
        VALUES %s
        ON CONFLICT ({conflict})
        DO NOTHING
        """
    else:
        sql = f"INSERT INTO {table} ({cols}) VALUES %s"
    try:
        # Use the passed db_conn
        with db_conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=min(len(rows), 500))
        db_conn.commit() # Commit changes
        logger.debug(f"Successfully committed batch of {len(rows)} rows to table {table}.") # New log
        return len(rows)
    except Exception as e:
        db_conn.rollback() # Rollback on error
        logger.error("DB write failed: %s", e, exc_info=True)
        return 0


def process_message(topic: str, payload: Any, connection_string: str, db_conn: Any, app_config: Dict[str, Any]) -> Dict[str, Any]: # Add app_config
    """
    Route one MQTT message to the correct table and write. Used by both MQTT loop and Lambda.
    Returns {"table": str, "written": int} or {"error": str}.
    """
    logger.debug(f"Received MQTT message for topic: {topic}") # New debug log
    
    dlq_enabled = app_config.get("dlq_enabled", False)
    dlq_file_path = app_config.get("dlq_file_path")
    dlq_max_size_mb = app_config.get("dlq_max_size_mb")

    if not (connection_string or "").strip():
        logger.error("DATABASE_URL is empty; cannot write to database")
        return {"table": "", "written": 0}

    config = get_table_config_for_topic(topic)
    if not config:
        reason = "No table config found for topic"
        logger.info(f"Topic {topic}: {reason} (expected suffix: meter_data, alarms, alarm_history, or locations)")
        if dlq_enabled:
            _write_to_dlq(dlq_file_path, dlq_max_size_mb, topic, payload, reason)
        return {"table": "", "written": 0}

    table, columns, row_mapper, conflict_cols, update_cols = config
    rows_data = parse_payload(payload)
    if not rows_data:
        reason = "Payload could not be parsed as JSON list/object"
        logger.warning(f"Topic {topic}: {reason}")
        if dlq_enabled:
            _write_to_dlq(dlq_file_path, dlq_max_size_mb, topic, payload, reason)
        return {"table": table, "written": 0}


    rows: List[Tuple] = []
    for item in rows_data:
        row = row_mapper(item)
        if row is not None:
            rows.append(row)
        else:
            reason = f"Row mapper rejected item (missing/invalid fields): {item}"
            logger.warning(reason)
            if dlq_enabled:
                _write_to_dlq(dlq_file_path, dlq_max_size_mb, topic, item, reason) # Use original item for DLQ

    if not rows:
        # Log first row's keys and sample values so we can see why mapper rejected
        first = rows_data[0] if rows_data else {}
        reason = (f"All {len(rows_data)} row(s) were skipped by mapper. "
                  f"First row keys: {list(first.keys())}; required for telemetry: time, site_id, source_id, register_name, device_id, value")
        logger.warning(f"Topic {topic} -> {table}: {reason}")
        if dlq_enabled:
            _write_to_dlq(dlq_file_path, dlq_max_size_mb, topic, payload, reason) # Use original payload for DLQ
        return {"table": table, "written": 0}

    # Pass db_conn instead of connection_string
    written = write_batch_to_db(db_conn, table, columns, rows, conflict_cols, update_cols)
    if written:
        logger.info("Inserted %s rows to table %s", written, table)
    else:
        # write_batch_to_db already logs error; write to DLQ if enabled
        if dlq_enabled:
            _write_to_dlq(dlq_file_path, dlq_max_size_mb, topic, payload, "DB write failed")
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
        "dlq_enabled": os.environ.get("DLQ_ENABLED", "false").strip().lower() in ("1", "true", "yes"),
        "dlq_file_path": os.environ.get("DLQ_FILE_PATH", "dlq_failed_messages.log"),
        "dlq_max_size_mb": int(os.environ.get("DLQ_MAX_SIZE_MB", "100")),
    }


def run_ingest(config: Dict[str, Any]) -> None:
    """Run MQTT client: subscribe to topic(s), on message route to table and write."""
    import paho.mqtt.client as mqtt
    import psycopg2 # Import psycopg2 here

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

    # Establish persistent database connection
    try:
        db_conn = psycopg2.connect(db_url)
        db_conn.autocommit = False # Manage transactions explicitly
        logger.info("✅ Established persistent PostgreSQL connection.")
    except Exception as e:
        logger.error(f"❌ Failed to establish persistent PostgreSQL connection: {e}", exc_info=True)
        sys.exit(1)

    # Perform schema validation for all tables
    for _, table_config in TABLE_ROUTERS:
        table_name = table_config[0]
        expected_columns = table_config[1]
        if not _validate_table_schema(db_conn, table_name, expected_columns):
            logger.critical(f"Aborting due to schema validation failure for table '{table_name}'.")
            db_conn.close()
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
        # Pass the persistent connection to process_message
        result = process_message(msg.topic, msg.payload, db_url, db_conn, config)
        w = result.get("written", 0)
        if w:
            logger.info("Topic %s -> %s: inserted %s rows", msg.topic, result.get("table", ""), w)
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
        logger.info("Shutdown. Closing database connection.")
        db_conn.close() # Close connection on shutdown
        client.disconnect()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    client.loop_forever()


def main() -> None:
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run_ingest(get_config())


if __name__ == "__main__":
    main()
