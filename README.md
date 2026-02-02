# MQTT-to-DB Ingest (standalone)

Independent service that runs on a **private server**, subscribes to an MQTT broker, and writes incoming messages to PostgreSQL tables (telemetry, alarms, alarm_history, locations).

No other project code is required. Copy this folder to the server, configure env, install deps, and run.

## Requirements

- Python 3.8+
- PostgreSQL reachable from the server
- MQTT broker (e.g. Mosquitto) reachable from the server

## Setup on the server

1. **Copy this folder** to the server (e.g. `/opt/mqtt_to_db` or `~/mqtt_to_db`).

2. **Create a virtualenv and install dependencies:**
   ```bash
   cd /path/to/database\ writer
   python3 -m venv venv
   source venv/bin/activate   # Linux/macOS
   pip install -r requirements.txt
   ```

3. **Configure environment** – either a `.env` file in this directory or system/env vars:

   | Variable | Required | Default | Description |
   |----------|----------|---------|-------------|
   | `DATABASE_URL` | Yes | — | PostgreSQL connection string (e.g. `postgresql://user:pass@host:5432/dbname`) |
   | `MQTT_BROKER_HOST` | Yes | `localhost` | MQTT broker hostname |
   | `MQTT_BROKER_PORT` | No | `1883` | MQTT broker port |
   | `MQTT_TOPIC` | No | `ems/site/+#` | Topic(s) to subscribe to (e.g. `ems/site/#`) |
   | `MQTT_USERNAME` | No | — | Broker username |
   | `MQTT_PASSWORD` | No | — | Broker password |
   | `MQTT_USE_TLS` | No | — | Set to `1`, `true`, or `yes` for TLS |
   | `MQTT_CLIENT_ID` | No | `mqtt_to_db` | MQTT client ID (useful if running multiple clients) |
   | `DB_BATCH_SIZE` | No | `500` | Batch size for DB writes (internal) |

   Example `.env`:
   ```bash
   cp .env.example .env
   # Edit .env with your broker and database settings
   ```

4. **Run:**
   ```bash
   python mqtt_to_db.py
   ```
   Or make executable and run:
   ```bash
   chmod +x mqtt_to_db.py
   ./mqtt_to_db.py
   ```

   Stop with `Ctrl+C` or `SIGTERM`; the process exits cleanly.

## Run as a systemd service (optional)

1. Copy the example unit and edit paths/user:
   ```bash
   sudo cp mqtt_to_db.service.example /etc/systemd/system/mqtt_to_db.service
   sudo nano /etc/systemd/system/mqtt_to_db.service
   ```
   Set `WorkingDirectory` and `ExecStart` to your install path, and `User` if needed.

2. Enable and start:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable mqtt_to_db
   sudo systemctl start mqtt_to_db
   sudo systemctl status mqtt_to_db
   ```

3. Logs:
   ```bash
   journalctl -u mqtt_to_db -f
   ```

## Topic → table mapping

| Topic suffix | Table |
|--------------|--------|
| `meter_data` | `telemetry` |
| `alarms` | `alarms` |
| `alarm_history` | `alarm_history` |
| `locations` | `locations` |

Messages are expected as JSON on these topics (under the subscribed prefix, e.g. `ems/site/1/meter_data`). The script routes by the last path segment.
# database_writer
