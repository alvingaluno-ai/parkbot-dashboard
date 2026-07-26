"""
app.py — Parkbot vehicle counter backend + dashboard.

Ingests finalized crossing events POSTed directly by an ESP32 running
parkbot_treadle_firmware_v5.ino (device_id / event / timestamp / vehicle_type,
X-Device-Key header), stores them in SQLite, and serves a live-updating
dashboard.

Note on architecture vs. the original Tuya-cloud build this replaces:
the treadle-plate firmware already does debounce, direction detection, and
axle-based vehicle classification on-device before it ever POSTs — so unlike
the old Tuya version there is no separate crossing-detection layer here
(no traffic_events.py equivalent). Each POST to /api/v1/events IS one
finalized vehicle, one row in the database. See README.md for the full
comparison and the mismatch vs. the original doc's simpler two-boolean-sensor
design.
"""

import json
import os
import queue
import threading
import time
from datetime import datetime, timezone, timedelta

from flask import Flask, request, jsonify, Response

from storage import EventStore

app = Flask(__name__)
store = EventStore(os.environ.get("DB_PATH", "parkbot.db"))

# Shared secret the ESP32 sends as the X-Device-Key header. Must match
# DEVICE_API_KEY in the firmware. Set a real value via environment variable
# in production — the fallback here is only for local bench testing.
DEVICE_API_KEY = os.environ.get("DEVICE_API_KEY", "REPLACE_WITH_PER_DEVICE_API_KEY")

# A device is "online" if it's sent an event OR a heartbeat more recently
# than this. The firmware heartbeats every HEARTBEAT_INTERVAL_MS (30s by
# default) independent of vehicle traffic, so a quiet-but-alive device still
# shows online — this window just needs to survive a couple of missed
# heartbeats, not a long traffic lull.
ONLINE_WINDOW_SECONDS = int(os.environ.get("ONLINE_WINDOW_SECONDS", "90"))

# Heartbeats are kept in memory only (not written to SQLite) — they're a
# "still alive" signal, not history worth persisting. Resets on restart/
# redeploy are fine: the device re-establishes it within one heartbeat
# interval. device_id -> last-heartbeat unix seconds.
_heartbeats = {}
_heartbeats_lock = threading.Lock()

# In-process pub/sub so the dashboard can be pushed updates instead of
# polling on a timer. Each connected browser tab gets its own Queue; any
# ingested event or heartbeat drops a tiny "something changed" message into
# every queue, and the browser refetches /api/status + /api/logs the moment
# it receives one. Works because Render's free tier + this Procfile run a
# single worker process, so all queues live in the same memory space — this
# would need a real pub/sub (e.g. Redis) if the service ever scales to
# multiple worker processes.
_subscribers = set()
_subscribers_lock = threading.Lock()


def broadcast(kind):
    msg = json.dumps({"type": kind, "t": time.time()})
    with _subscribers_lock:
        dead = []
        for q in _subscribers:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _subscribers.discard(q)

# Philippines = UTC+8, matching GMT_OFFSET_SEC in the firmware. Used only to
# compute "today's" totals on a local-midnight boundary.
LOCAL_OFFSET = timezone(timedelta(hours=8))

VALID_EVENTS = {"IN", "OUT"}
VALID_TYPES = {"motorcycle", "car", "large"}


def local_midnight_epoch():
    now_local = datetime.now(LOCAL_OFFSET)
    midnight_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(midnight_local.timestamp())


def require_device_key(req):
    return req.headers.get("X-Device-Key", "") == DEVICE_API_KEY


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------
@app.route("/api/v1/events", methods=["POST"])
def api_ingest_event():
    if not require_device_key(request):
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(silent=True, force=True) or {}

    device_id = body.get("device_id")
    event = body.get("event")
    vehicle_type = body.get("vehicle_type")
    raw_ts = body.get("timestamp")

    if not device_id or event not in VALID_EVENTS or vehicle_type not in VALID_TYPES:
        return jsonify({"error": "malformed event", "body": body}), 400

    ts_epoch = None
    uptime_ms = None
    if isinstance(raw_ts, (int, float)):
        ts_epoch = int(raw_ts)
    elif isinstance(raw_ts, str) and raw_ts.startswith("uptime_ms:"):
        try:
            uptime_ms = int(raw_ts.split(":", 1)[1])
        except ValueError:
            pass

    row_id = store.record_event(device_id, event, vehicle_type, ts_epoch, uptime_ms)
    broadcast("event")
    return jsonify({"ok": True, "id": row_id}), 201


@app.route("/api/v1/heartbeat", methods=["POST"])
def api_heartbeat():
    if not require_device_key(request):
        return jsonify({"error": "unauthorized"}), 401

    body = request.get_json(silent=True, force=True) or {}
    device_id = body.get("device_id")
    if not device_id:
        return jsonify({"error": "device_id required"}), 400

    with _heartbeats_lock:
        _heartbeats[device_id] = time.time()
    broadcast("heartbeat")
    return jsonify({"ok": True}), 200


# ---------------------------------------------------------------------------
# Dashboard data
# ---------------------------------------------------------------------------
@app.route("/api/status", methods=["GET"])
def api_status():
    today_totals = store.counts_since(local_midnight_epoch())
    all_time_totals = store.counts_since(None)

    devices = []
    with _heartbeats_lock:
        heartbeat_devices = set(_heartbeats.keys())
    for device_id in (set(store.known_devices()) | heartbeat_devices) or {"gate-01-treadle"}:
        event_seen = store.last_seen(device_id)
        with _heartbeats_lock:
            heartbeat_seen = _heartbeats.get(device_id)
        candidates = [t for t in (event_seen, heartbeat_seen) if t is not None]
        seen = max(candidates) if candidates else None
        online = seen is not None and (time.time() - seen) < ONLINE_WINDOW_SECONDS
        devices.append({
            "device_id": device_id,
            "last_seen": seen,
            "seconds_ago": (int(time.time() - seen) if seen is not None else None),
            "online": online,
        })

    return jsonify({
        "today": today_totals,
        "all_time": all_time_totals,
        "devices": devices,
        "server_time": int(time.time()),
    })


@app.route("/api/logs", methods=["GET"])
def api_logs():
    limit = min(int(request.args.get("limit", 50)), 200)
    return jsonify({"events": store.recent_events(limit)})


@app.route("/api/stream", methods=["GET"])
def api_stream():
    """Server-Sent Events stream. Each message just means 'something changed
    — go refetch /api/status and /api/logs.' Keeping the payload tiny and
    reusing the existing REST endpoints avoids having two different code
    paths compute the same numbers."""
    q = queue.Queue(maxsize=20)
    with _subscribers_lock:
        _subscribers.add(q)

    def gen():
        try:
            yield "data: {\"type\": \"connected\"}\n\n"
            while True:
                try:
                    msg = q.get(timeout=15)
                    yield f"data: {msg}\n\n"
                except queue.Empty:
                    yield ": keepalive\n\n"  # prevents idle proxies from closing the connection
        finally:
            with _subscribers_lock:
                _subscribers.discard(q)

    return Response(
        gen(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Dashboard page
# ---------------------------------------------------------------------------
PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Parkbot · Gate-01 Treadle</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
  :root{
    --bg:#12151b;
    --panel:#1a1f28;
    --panel-2:#1f2530;
    --border:#2a3140;
    --text:#e7eaf0;
    --muted:#7c8698;
    --amber:#ffb238;
    --amber-dim:#8a6323;
    --teal:#3fd6c0;
    --coral:#ff6b6b;
    --green:#4ade80;
    --red:#f2545b;
  }
  *{box-sizing:border-box;}
  body{
    margin:0;
    background:
      radial-gradient(circle at 15% 0%, #1a2130 0%, transparent 45%),
      radial-gradient(circle at 85% 100%, #1a1c28 0%, transparent 40%),
      var(--bg);
    color:var(--text);
    font-family:'Inter',sans-serif;
    min-height:100vh;
    padding:28px 20px 60px;
  }
  .wrap{max-width:960px;margin:0 auto;}

  .topbar{
    display:flex;justify-content:space-between;align-items:flex-start;
    flex-wrap:wrap;gap:14px;margin-bottom:26px;
  }
  .brand{display:flex;flex-direction:column;gap:4px;}
  .brand .eyebrow{
    font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);
  }
  .brand h1{
    margin:0;font-size:22px;font-weight:700;letter-spacing:.01em;
  }
  .status-pill{
    display:flex;align-items:center;gap:8px;
    background:var(--panel);border:1px solid var(--border);
    padding:8px 14px;border-radius:999px;font-size:13px;color:var(--muted);
  }
  .dot{width:8px;height:8px;border-radius:50%;background:var(--muted);flex:none;}
  .dot.online{background:var(--green);box-shadow:0 0 8px var(--green);}
  .dot.offline{background:var(--red);box-shadow:0 0 8px var(--red);}

  .hero{
    background:linear-gradient(180deg,var(--panel-2),var(--panel));
    border:1px solid var(--border);
    border-radius:16px;
    padding:32px 28px;
    display:grid;
    grid-template-columns:1fr auto 1fr;
    align-items:center;
    gap:20px;
    margin-bottom:20px;
  }
  @media(max-width:640px){ .hero{grid-template-columns:1fr;text-align:center;} }

  .tally{display:flex;flex-direction:column;gap:8px;}
  .tally.out{align-items:flex-end;}
  @media(max-width:640px){ .tally.out{align-items:center;} }
  .tally .label{
    font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);
    display:flex;align-items:center;gap:6px;
  }
  .tally.in .label{color:var(--teal);}
  .tally.out .label{color:var(--coral);}

  .odometer{display:flex;gap:3px;}
  .odometer.small .digit{width:22px;height:34px;font-size:18px;}
  .digit{
    width:30px;height:46px;
    background:linear-gradient(180deg,#0e1115,#161a21 48%,#0e1115 52%,#161a21);
    border:1px solid #333c4d;
    border-radius:4px;
    display:flex;align-items:center;justify-content:center;
    font-family:'Space Mono',monospace;font-weight:700;font-size:24px;
    color:var(--amber);
    text-shadow:0 0 10px rgba(255,178,56,.45);
    position:relative;
    overflow:hidden;
  }
  .digit::after{
    content:"";position:absolute;left:0;right:0;top:50%;height:1px;
    background:rgba(0,0,0,.55);
  }
  .tally.in .odometer .digit{color:var(--teal);text-shadow:0 0 10px rgba(63,214,192,.45);}
  .tally.out .odometer .digit{color:var(--coral);text-shadow:0 0 10px rgba(255,107,107,.45);}

  .inside{display:flex;flex-direction:column;align-items:center;gap:10px;}
  .inside .label{font-size:11px;letter-spacing:.14em;text-transform:uppercase;color:var(--muted);}
  .inside .odometer .digit{width:38px;height:58px;font-size:30px;}
  .inside .sub{font-size:12px;color:var(--muted);}

  .grid2{display:grid;grid-template-columns:1.1fr .9fr;gap:20px;}
  @media(max-width:720px){ .grid2{grid-template-columns:1fr;} }

  .panel{
    background:var(--panel);border:1px solid var(--border);border-radius:14px;
    padding:20px 22px;
  }
  .panel h2{
    margin:0 0 14px;font-size:13px;letter-spacing:.1em;text-transform:uppercase;
    color:var(--muted);font-weight:600;
  }

  .bar-row{display:flex;align-items:center;gap:10px;margin-bottom:12px;}
  .bar-row:last-child{margin-bottom:0;}
  .bar-row .name{width:78px;font-size:13px;color:var(--muted);flex:none;}
  .bar-track{flex:1;height:10px;background:#11151c;border-radius:6px;overflow:hidden;border:1px solid var(--border);}
  .bar-fill{height:100%;border-radius:6px;transition:width .4s ease;}
  .bar-fill.motorcycle{background:var(--teal);}
  .bar-fill.car{background:var(--amber);}
  .bar-fill.large{background:var(--coral);}
  .bar-row .count{width:28px;text-align:right;font-family:'Space Mono',monospace;font-size:13px;flex:none;}

  .ticker{max-height:340px;overflow-y:auto;display:flex;flex-direction:column;gap:0;}
  .tick{
    display:flex;align-items:center;gap:10px;
    padding:10px 0;border-bottom:1px solid var(--border);font-size:13px;
  }
  .tick:last-child{border-bottom:none;}
  .tick .arrow{width:22px;flex:none;text-align:center;font-weight:700;font-family:'Space Mono',monospace;}
  .tick.in .arrow{color:var(--teal);}
  .tick.out .arrow{color:var(--coral);}
  .tick .vt{
    flex:none;padding:2px 8px;border-radius:999px;font-size:11px;
    text-transform:uppercase;letter-spacing:.05em;background:var(--panel-2);color:var(--muted);
    border:1px solid var(--border);
  }
  .tick .time{margin-left:auto;color:var(--muted);font-family:'Space Mono',monospace;font-size:12px;flex:none;}
  .empty{color:var(--muted);font-size:13px;padding:8px 0;}

  ::-webkit-scrollbar{width:8px;}
  ::-webkit-scrollbar-thumb{background:var(--border);border-radius:4px;}

  footer{
    text-align:center;color:var(--muted);font-size:12px;margin-top:24px;
  }
</style>
</head>
<body>
<div class="wrap">

  <div class="topbar">
    <div class="brand">
      <span class="eyebrow">Parkbot · Live Vehicle Counter</span>
      <h1>Gate-01 Treadle Plate</h1>
    </div>
    <div class="status-pill">
      <span class="dot" id="statusDot"></span>
      <span id="statusText">Connecting…</span>
    </div>
  </div>

  <div class="hero">
    <div class="tally in">
      <span class="label">↓ In today</span>
      <div class="odometer" id="odoIn"></div>
    </div>
    <div class="inside">
      <span class="label">Currently inside</span>
      <div class="odometer" id="odoInside"></div>
      <span class="sub" id="allTimeSub">— all-time crossings</span>
    </div>
    <div class="tally out">
      <span class="label">Out today ↑</span>
      <div class="odometer" id="odoOut"></div>
    </div>
  </div>

  <div class="grid2">
    <div class="panel">
      <h2>Live event feed</h2>
      <div class="ticker" id="ticker">
        <div class="empty">Waiting for the first crossing…</div>
      </div>
    </div>

    <div class="panel">
      <h2>In today, by vehicle type</h2>
      <div class="bar-row">
        <span class="name">Motorcycle</span>
        <div class="bar-track"><div class="bar-fill motorcycle" id="barMoto" style="width:0%"></div></div>
        <span class="count" id="countMoto">0</span>
      </div>
      <div class="bar-row">
        <span class="name">Car</span>
        <div class="bar-track"><div class="bar-fill car" id="barCar" style="width:0%"></div></div>
        <span class="count" id="countCar">0</span>
      </div>
      <div class="bar-row">
        <span class="name">Large</span>
        <div class="bar-track"><div class="bar-fill large" id="barLarge" style="width:0%"></div></div>
        <span class="count" id="countLarge">0</span>
      </div>
    </div>
  </div>

  <footer>Live · updates instantly on each crossing · today resets at local midnight (UTC+8)</footer>
</div>

<script>
function renderOdometer(el, value, digits){
  const str = String(Math.max(0, value)).padStart(digits, '0');
  el.innerHTML = '';
  for(const ch of str){
    const d = document.createElement('div');
    d.className = 'digit';
    d.textContent = ch;
    el.appendChild(d);
  }
}

function fmtAgo(seconds){
  if(seconds === null || seconds === undefined) return 'never';
  if(seconds < 5) return 'just now';
  if(seconds < 60) return seconds + 's ago';
  if(seconds < 3600) return Math.floor(seconds/60) + 'm ago';
  return Math.floor(seconds/3600) + 'h ago';
}

function fmtTime(row){
  let ts = row.ts_epoch ? row.ts_epoch * 1000 : row.received_at * 1000;
  const d = new Date(ts);
  return d.toLocaleTimeString('en-US', {hour:'2-digit', minute:'2-digit', second:'2-digit'});
}

async function pollStatus(){
  try{
    const res = await fetch('/api/status');
    const data = await res.json();

    const t = data.today;
    renderOdometer(document.getElementById('odoIn'), t.in, 3);
    renderOdometer(document.getElementById('odoOut'), t.out, 3);
    renderOdometer(document.getElementById('odoInside'), Math.max(0, t.inside), 3);
    document.getElementById('allTimeSub').textContent =
      data.all_time.total.toLocaleString() + ' all-time crossings';

    const byType = t.by_type || {};
    const maxType = Math.max(1, byType.motorcycle||0, byType.car||0, byType.large||0);
    document.getElementById('countMoto').textContent = byType.motorcycle||0;
    document.getElementById('countCar').textContent = byType.car||0;
    document.getElementById('countLarge').textContent = byType.large||0;
    document.getElementById('barMoto').style.width = (100*(byType.motorcycle||0)/maxType)+'%';
    document.getElementById('barCar').style.width = (100*(byType.car||0)/maxType)+'%';
    document.getElementById('barLarge').style.width = (100*(byType.large||0)/maxType)+'%';

    const dot = document.getElementById('statusDot');
    const text = document.getElementById('statusText');
    const dev = (data.devices && data.devices[0]) || null;
    if(dev && dev.online){
      dot.className = 'dot online';
      text.textContent = dev.device_id + ' · online · seen ' + fmtAgo(dev.seconds_ago);
    } else if (dev){
      dot.className = 'dot offline';
      text.textContent = dev.device_id + ' · quiet · seen ' + fmtAgo(dev.seconds_ago);
    } else {
      dot.className = 'dot';
      text.textContent = 'No events received yet';
    }
  }catch(e){
    document.getElementById('statusDot').className = 'dot offline';
    document.getElementById('statusText').textContent = 'Dashboard cannot reach the backend';
  }
}

async function pollLogs(){
  try{
    const res = await fetch('/api/logs?limit=25');
    const data = await res.json();
    const ticker = document.getElementById('ticker');
    if(!data.events || data.events.length === 0){
      ticker.innerHTML = '<div class="empty">Waiting for the first crossing…</div>';
      return;
    }
    ticker.innerHTML = '';
    for(const row of data.events){
      const div = document.createElement('div');
      div.className = 'tick ' + (row.event === 'IN' ? 'in' : 'out');
      div.innerHTML =
        '<span class="arrow">' + (row.event === 'IN' ? '↓' : '↑') + '</span>' +
        '<span class="vt">' + row.vehicle_type + '</span>' +
        '<span class="time">' + fmtTime(row) + '</span>';
      ticker.appendChild(div);
    }
  }catch(e){ /* keep last known feed on transient failure */ }
}

function tick(){ pollStatus(); pollLogs(); }
tick();

// Instant updates: the backend pushes a tiny "something changed" message
// over this connection the moment an event or heartbeat comes in, and we
// just refetch the normal REST endpoints. EventSource reconnects on its
// own if the connection drops (e.g. Render free tier waking from idle).
function connectStream(){
  const es = new EventSource('/api/stream');
  es.onmessage = () => tick();
}
connectStream();

// Safety net only — covers the rare case where the SSE connection silently
// stalls without firing an error. Everyday updates come from the stream
// above, not this timer.
setInterval(tick, 15000);
</script>
</body>
</html>"""


@app.route("/", methods=["GET"])
def dashboard():
    return Response(PAGE, mimetype="text/html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)