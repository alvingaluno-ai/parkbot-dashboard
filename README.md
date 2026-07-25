# Parkbot Dashboard

A Flask + SQLite backend and live-updating web dashboard for the Gate-01
treadle-plate vehicle counter, receiving events directly from an ESP32 —
no Tuya cloud involved.

## Before you deploy — three things worth knowing

**1. This backend matches `parkbot_treadle_firmware_v5.ino`, not the §5
firmware sample in the original planning doc.** The doc's sample firmware
sends a raw boolean per sensor (`{"device":"entry","active":true}`) and
expects the *backend* to do crossing detection. Your actual firmware
(`parkbot_treadle_firmware_v5.ino`) is more capable than that: it already
debounces each plate, determines IN/OUT direction, groups multi-axle
vehicles into one event, and classifies vehicle type — all on-device —
before it ever POSTs. So this backend's `/api/v1/events` route matches
*that* firmware's payload instead:

```json
{"device_id": "gate-01-treadle", "event": "IN", "timestamp": 1721900000, "vehicle_type": "car"}
```

sent with an `X-Device-Key` header, to `/api/v1/events` (path already set
correctly in `BACKEND_URL` in your `.ino` file). One POST = one finalized
vehicle. There's no `traffic_events.py`-style dedup layer here because the
firmware already did that job.

**2. The firmware ships with networking disabled.** In
`parkbot_treadle_firmware_v5.ino`:
- `BACKEND_POST_DISABLED = true` — flip to `false` once `BACKEND_URL` points
  at your real deployed backend.
- `BACKEND_URL` is still `https://your-parkbot-backend.example.com/...` —
  update it to your Render URL, e.g. `https://your-app.onrender.com/api/v1/events`.
- `DEVICE_API_KEY` is still the placeholder string — set it to match the
  `DEVICE_API_KEY` environment variable you configure on the backend (see
  below). They must match exactly, or every POST gets a 401.
- The WiFi credentials in that file are your real home network credentials
  in plain text — don't commit that `.ino` file to a public repo as-is.

**3. The pinout reference in your project is for the ESP32-**S3** DevKitC**,
but the board in your photo (ESP-32 WROOM module, CP2102 USB-UART chip,
`VP`/`VN`/`EN` pin labels) is a plain **ESP32** DevKitC, not an S3 — different
chip, different pin map. Your firmware's `PLATE_A_PIN = 27` /
`PLATE_B_PIN = 26` are fine on this classic ESP32 board (both are broken out
GPIOs, as visible in your photo), but if you go pin-hunting for anything else,
use a classic ESP32 DevKitC pinout, not the S3 reference doc.

## What's in this folder

| File | Purpose |
|---|---|
| `app.py` | Flask routes: ingestion, status/logs APIs, dashboard page |
| `storage.py` | SQLite event store (`EventStore` class) |
| `requirements.txt` | Python deps |
| `Procfile` | Render/gunicorn start command |

## Run it locally

```bash
pip install -r requirements.txt
export DEVICE_API_KEY="pick-a-long-random-string"
python app.py
```

Dashboard: `http://localhost:5000`. It polls `/api/status` and `/api/logs`
every 3 seconds — no need to refresh the page.

Bench-test without hardware:

```bash
curl -X POST http://localhost:5000/api/v1/events \
  -H "Content-Type: application/json" \
  -H "X-Device-Key: pick-a-long-random-string" \
  -d '{"device_id":"gate-01-treadle","event":"IN","timestamp":1721900000,"vehicle_type":"car"}'
```

Refresh the dashboard (or just wait 3s) and you should see the count and
event feed update.

## Deploy to Render

1. Push this folder to a git repo.
2. New Web Service on Render, pointed at that repo. Build command:
   `pip install -r requirements.txt`. Start command is already in `Procfile`.
3. Set the environment variable `DEVICE_API_KEY` to the same long random
   string you'll put in the firmware's `DEVICE_API_KEY`.
4. Once deployed, update the firmware:
   - `BACKEND_URL` → `https://<your-service>.onrender.com/api/v1/events`
   - `DEVICE_API_KEY` → the same string as step 3
   - `BACKEND_POST_DISABLED` → `false`
5. Reflash. Watch the Serial Monitor for `POST ok (201): ...` after a
   crossing, and watch the dashboard update live.

Render's free tier spins down on idle, so the first request after a quiet
period will be slow to wake it — the firmware's 5s HTTP timeout and
in-RAM retry queue already handle that gracefully (events queue and retry
rather than being dropped).

## API reference

| Route | Method | Auth | Purpose |
|---|---|---|---|
| `/api/v1/events` | POST | `X-Device-Key` header | Record one finalized vehicle crossing |
| `/api/status` | GET | none | Today's + all-time totals, by vehicle type, device online status |
| `/api/logs?limit=N` | GET | none | Most recent N events (default 50, max 200) |
| `/health` | GET | none | Liveness check |
| `/` | GET | none | Dashboard page |

## One gap worth knowing about: "online" status

The dashboard shows a device as **online** if it POSTed an event within the
last 10 minutes (`ONLINE_WINDOW_SECONDS`, configurable via env var). Unlike
the original doc's design, the current firmware has **no separate heartbeat
call** — it only POSTs when a vehicle actually crosses. That means during a
genuinely quiet stretch (say, an empty parking lot overnight), the dashboard
will show the device as "quiet" even though it's still running fine and just
hasn't seen a vehicle. If you want a true up/down signal independent of
traffic, the cleanest fix is adding a small periodic heartbeat POST to the
firmware (same pattern as `sendHeartbeat()` in the original planning doc's
§5 sample) and a matching `/api/v1/heartbeat` route here — happy to add that
if you want tighter offline detection.

## Extending

- **Multiple gates/devices**: the schema already stores `device_id` per row,
  so a second ESP32 posting a different `device_id` shows up automatically
  in `/api/status`'s `devices` list and gets its own bar in future UI work —
  no schema change needed.
- **Persistence across deploys**: SQLite lives in a single file
  (`parkbot.db` by default, `DB_PATH` env var to change it). Render's free
  tier filesystem is ephemeral on redeploy — if you need the count history
  to survive redeploys, attach a Render persistent disk or move to a hosted
  Postgres instance.
