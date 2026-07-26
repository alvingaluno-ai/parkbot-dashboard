"""
storage.py — SQLite-backed event store for the Parkbot vehicle counter.

Schema is deliberately flat: one row per finalized vehicle crossing, as
POSTed by the ESP32 firmware (parkbot_treadle_firmware_v5.ino). The
treadle-plate firmware already does debounce, direction detection, and
axle-based vehicle classification on-device, so there is no
Tuya-era-style crossing-detection logic in this backend — a row here IS
a finalized event, one per vehicle.
"""

import sqlite3
import time
from contextlib import contextmanager

DB_PATH = "parkbot.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id     TEXT    NOT NULL,
    event         TEXT    NOT NULL,       -- 'IN' or 'OUT'
    vehicle_type  TEXT    NOT NULL,       -- 'motorcycle' | 'car' | 'large'
    ts_epoch      INTEGER,                -- unix seconds from device NTP, NULL if unsynced
    uptime_ms     INTEGER,                -- device millis() fallback, NULL if not used
    received_at   INTEGER NOT NULL        -- server-side unix seconds, always present
);
CREATE INDEX IF NOT EXISTS idx_events_received_at ON events(received_at);
CREATE INDEX IF NOT EXISTS idx_events_device ON events(device_id);
"""


class EventStore:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        with self._conn() as conn:
            conn.executescript(SCHEMA)

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def record_event(self, device_id, event, vehicle_type, ts_epoch=None, uptime_ms=None):
        with self._conn() as conn:
            cur = conn.execute(
                """INSERT INTO events (device_id, event, vehicle_type, ts_epoch, uptime_ms, received_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (device_id, event, vehicle_type, ts_epoch, uptime_ms, int(time.time())),
            )
            return cur.lastrowid

    def recent_events(self, limit=50):
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY received_at DESC, id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def counts_since(self, since_epoch=None):
        """Aggregate IN/OUT/type counts, optionally restricted to received_at >= since_epoch."""
        with self._conn() as conn:
            if since_epoch is None:
                rows = conn.execute("SELECT event, vehicle_type FROM events").fetchall()
            else:
                rows = conn.execute(
                    "SELECT event, vehicle_type FROM events WHERE received_at >= ?",
                    (since_epoch,),
                ).fetchall()

        totals = {"IN": 0, "OUT": 0}
        # Vehicle-type breakdown counts arrivals only (event == "IN"). A car
        # that enters and later exits is one vehicle, not two — counting
        # both IN and OUT here would double the total for every completed
        # visit, which is misleading for "how many cars came in today."
        by_type = {"motorcycle": 0, "car": 0, "large": 0}
        for r in rows:
            ev = r["event"]
            vt = r["vehicle_type"]
            if ev in totals:
                totals[ev] += 1
            if ev == "IN":
                by_type[vt] = by_type.get(vt, 0) + 1

        return {
            "in": totals["IN"],
            "out": totals["OUT"],
            "inside": totals["IN"] - totals["OUT"],
            "total": totals["IN"] + totals["OUT"],
            "by_type": by_type,
        }

    def last_seen(self, device_id=None):
        """Most recent received_at, overall or for one device. None if no events yet."""
        with self._conn() as conn:
            if device_id:
                row = conn.execute(
                    "SELECT MAX(received_at) AS ts FROM events WHERE device_id = ?",
                    (device_id,),
                ).fetchone()
            else:
                row = conn.execute("SELECT MAX(received_at) AS ts FROM events").fetchone()
            return row["ts"] if row and row["ts"] is not None else None

    def known_devices(self):
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT device_id FROM events ORDER BY device_id"
            ).fetchall()
            return [r["device_id"] for r in rows]