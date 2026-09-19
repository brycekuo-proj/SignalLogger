# SignalLogger

Android-first research logger for the TaipeiSignal project. SignalLogger is not a navigation app and does not provide driving advice. It collects raw road/GNSS/sensor observations for later map-matching, multi-level-road and signal-phase calibration work.

## v0.1.2 MVP

- Android 6+; primary first test device: OPPO A72 / Android 11.
- Foreground location service so logging can continue with the screen off.
- Background-recording mode uses a partial CPU wake lock while recording, requests battery-optimization exemption, and uses `START_STICKY` session recovery after system/vendor service kills.
- GPS location samples with wall-clock and monotonic timestamps, accuracy, speed, bearing and altitude when available.
- GNSS satellite quality snapshots on Android 7+.
- Optional accelerometer, gyroscope, rotation-vector, magnetometer and barometer logging; missing sensors do not block recording.
- Local SQLite is always written first and remains the source of truth during network outages.
- Realtime MCP `tools/call` batching to a configurable Mac endpoint, with sequence-based ACK and retry-safe/idempotent ingestion.
- v0.1.2 adds server-state reconciliation (`signal.sync_status`), lost-ACK recovery, multi-batch catch-up, strict MCP result validation, and visible ACK/last-success diagnostics in the Android UI.
- Manual `.signalzip` export containing the local SQLite database and manifest.
- Minimal engineering UI only.

Road name/direction/structure matching is intentionally not yet enabled in v0.1.1. The first road tests are intended to validate raw data quality before moving the matcher onto the phone.

## Build

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
./gradlew assembleDebug
```

APK:

```text
app/build/outputs/apk/debug/app-debug.apk
```

## Mac MCP receiver

The repository includes a small local receiver with no third-party Python dependencies:

```bash
export SIGNALLOGGER_TOKEN='choose-a-long-random-token'
python3 tools/signal_mcp_server.py \
  --host 127.0.0.1 \
  --port 18765 \
  --db "$HOME/Bryce AI Studio/SignalLoggerData/signal_ingest.sqlite"
```

The phone uses an endpoint ending in `/mcp`. For use away from the Mac's LAN, expose this local endpoint through an authenticated HTTPS tunnel and paste that HTTPS MCP URL plus the bearer token into the app.

The receiver implements:

- `signal.start_session`
- `signal.append_batch`
- `signal.end_session`
- `signal.sync_status`
- `signal.health`
- `signal.hud_snapshot` — read-only realtime HUD snapshot for TaipeiSignalHUD using vehicle position, locked travel bearing and optional preferred intersection IDs.
- `signal.consistency_events` — read recent HUD-vs-OPPO motion conflict records for signal calibration.

The receiver supports a separate HUD-only bearer token. That token is scoped to `signal.health` and `signal.hud_snapshot` and cannot read raw logger sessions, consistency history, or call write/sync tools.

For calibration, each primary A37 HUD result is stored as a short `hud_observation`. The MCP receiver also carries the road-test timing calibration table (`signal_timing_calibration`), currently seeded with the field-confirmed +2 second phase adjustment. A per-intersection row overrides the `*` default.

Xinsheng Elevated Road now has an explicit MCP road-mode rule. When the receiver detects a north/south high-speed trajectory in the Xinsheng corridor (or recent colocated red-while-moving evidence), it latches `XINSHENG_ELEVATED` and maps the vehicle to the next exit terminal instead of surface-road signals. Southbound targets: 濱江、長春、長安、忠孝、濟南. Northbound targets: 長安、民生、北安、通河. Unsupported terminal timing remains `UNKNOWN/--` rather than inventing a countdown.

For calibration, each primary A37 HUD result is stored as a short `hud_observation`. The receiver compares it against OPPO `location` samples using device timestamps, position, heading and distance to the same intersection. A `RED_WHILE_MOVING` event is recorded only when the phones are colocated, the approach direction agrees, both are within about 55 m of the same signal, and OPPO has sustained movement evidence. Events are merged into 5-second buckets. Because OPPO batches can arrive several seconds later, every new location batch also replays the matching A37 observations so delayed uploads are not missed.

Raw received data is stored outside the repository by default. Do not commit raw GPS tracks.

## Data safety

SignalLogger deliberately does not collect account names, contacts, IMEI, advertising IDs, microphone or camera data. A random app-install UUID is used as the device identifier. Raw trajectories stay on the phone and/or the private Mac receiver. GitHub should contain only code, schemas, documentation and safe aggregate/derived datasets.
