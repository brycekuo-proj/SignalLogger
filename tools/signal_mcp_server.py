#!/usr/bin/env python3
import argparse
import json
import math
import os
import sqlite3
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DB_LOCK = threading.Lock()


@contextmanager
def db_connection(path):
    """Open one SQLite connection and always close its file descriptors."""
    con = sqlite3.connect(path)
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


TOOLS = [
    {
        "name": "signal.start_session",
        "description": "Register a SignalLogger session on the Mac.",
        "inputSchema": {"type": "object", "required": ["session_id"], "additionalProperties": True},
    },
    {
        "name": "signal.append_batch",
        "description": "Append an idempotent batch of raw SignalLogger samples.",
        "inputSchema": {
            "type": "object",
            "required": ["session_id", "stream_type", "first_seq", "last_seq", "samples"],
            "properties": {
                "session_id": {"type": "string"},
                "stream_type": {"type": "string"},
                "first_seq": {"type": "integer"},
                "last_seq": {"type": "integer"},
                "samples": {"type": "array"},
            },
        },
    },
    {
        "name": "signal.end_session",
        "description": "Close a SignalLogger session.",
        "inputSchema": {"type": "object", "required": ["session_id"], "additionalProperties": True},
    },
    {
        "name": "signal.sync_status",
        "description": "Return highest received sequence per stream for a session.",
        "inputSchema": {"type": "object", "required": ["session_id"]},
    },
    {
        "name": "signal.health",
        "description": "Health check for the SignalLogger Mac receiver.",
        "inputSchema": {"type": "object"},
    },
    {
        "name": "signal.hud_snapshot",
        "description": "Return a realtime traffic-signal HUD snapshot for a vehicle position and travel bearing.",
        "inputSchema": {
            "type": "object",
            "required": ["latitude", "longitude", "bearing_deg"],
            "properties": {
                "latitude": {"type": "number"},
                "longitude": {"type": "number"},
                "bearing_deg": {"type": "number"},
                "speed_mps": {"type": "number"},
                "accuracy_m": {"type": "number"},
                "limit": {"type": "integer"},
                "intersection_ids": {
                    "type": "array",
                    "items": {"type": "string"}
                },
            },
        },
    },
]


def init_db(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with db_connection(path) as con:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ingest_session (
                session_id TEXT PRIMARY KEY,
                started_at_utc_ms INTEGER,
                ended_at_utc_ms INTEGER,
                state TEXT,
                device_install_id TEXT,
                device_model TEXT,
                android_version TEXT,
                app_version TEXT,
                sensor_capabilities TEXT,
                start_payload_json TEXT NOT NULL,
                end_payload_json TEXT,
                server_first_seen_ms INTEGER NOT NULL,
                server_last_seen_ms INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_sample (
                session_id TEXT NOT NULL,
                stream_type TEXT NOT NULL,
                seq INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                device_utc_ms INTEGER,
                server_received_ms INTEGER NOT NULL,
                PRIMARY KEY (session_id, stream_type, seq)
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS idx_raw_sample_time ON raw_sample(session_id, stream_type, seq)")


class HudModel:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.intersections = []
        self.intersections_by_id = {}
        self.plans = {}
        self.schedules = {}
        self.tz = ZoneInfo("Asia/Taipei")
        self._load()

    def _load(self):
        intersections_path = self.data_dir / "intersections.psv"
        plans_path = self.data_dir / "signal_plans.psv"
        schedule_path = self.data_dir / "signal_schedule.psv"

        with intersections_path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip() or line.startswith("#"):
                    continue
                p = line.rstrip("\n").split("|")
                if len(p) < 4:
                    continue
                try:
                    item = {
                        "id": p[0].strip(),
                        "name": " ".join(p[1].replace("\u3000", " ").split()),
                        "lon": float(p[2]),
                        "lat": float(p[3]),
                    }
                    self.intersections.append(item)
                    self.intersections_by_id[item["id"]] = item
                except Exception:
                    continue

        with plans_path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip() or line.startswith("#"):
                    continue
                p = line.rstrip("\n").split("|")
                if len(p) < 7:
                    continue
                try:
                    phases = []
                    for phase in p[6].split(";"):
                        v = phase.split(",")
                        if len(v) < 4:
                            continue
                        phases.append({
                            "green": int(v[0] or 0),
                            "yellow": int(v[1] or 0),
                            "allred": int(v[2] or 0),
                            "pedflash": int(v[3] or 0),
                        })
                    self.plans[(p[0], p[1])] = {
                        "direction": int(p[2] or 0),
                        "cycle": int(p[3] or 0),
                        "offset": int(p[4] or 0),
                        "phaseorder": p[5],
                        "phases": phases,
                    }
                except Exception:
                    continue

        with schedule_path.open("r", encoding="utf-8") as f:
            for line in f:
                if not line.strip() or line.startswith("#"):
                    continue
                p = line.rstrip("\n").split("|")
                if len(p) < 3:
                    continue
                try:
                    entries = []
                    for pair in p[2].split(","):
                        hhmm, plan_id = pair.split(":", 1)
                        v = int(hhmm)
                        entries.append(((v // 100) * 60 + (v % 100), plan_id))
                    self.schedules[(p[0], int(p[1]))] = entries
                except Exception:
                    continue

    @staticmethod
    def _distance_m(lat1, lon1, lat2, lon2):
        r = 6371000.0
        p1 = math.radians(lat1)
        p2 = math.radians(lat2)
        dp = math.radians(lat2 - lat1)
        dl = math.radians(lon2 - lon1)
        a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
        return r * 2.0 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1.0 - a)))

    @staticmethod
    def _bearing_deg(lat1, lon1, lat2, lon2):
        p1 = math.radians(lat1)
        p2 = math.radians(lat2)
        dl = math.radians(lon2 - lon1)
        y = math.sin(dl) * math.cos(p2)
        x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
        return math.degrees(math.atan2(y, x)) % 360.0

    @staticmethod
    def _angle_delta(a, b):
        return (b - a + 540.0) % 360.0 - 180.0

    @staticmethod
    def _cardinal_bucket(deg):
        return int(round((deg % 360.0) / 45.0)) & 7

    def _active_plan(self, icid, now):
        day = now.isoweekday()
        schedule = self.schedules.get((icid, day))
        if not schedule:
            return None, None
        minute = now.hour * 60 + now.minute
        active = schedule[0][1]
        for start, plan_id in schedule:
            if start <= minute:
                active = plan_id
            else:
                break
        return active, self.plans.get((icid, active))

    def _estimate(self, icid, bearing_deg, now):
        plan_id, plan = self._active_plan(icid, now)
        if not plan or plan["cycle"] <= 0 or not plan["phases"]:
            return None

        major = [
            i for i, ph in enumerate(plan["phases"])
            if ph["pedflash"] > 0 and ph["green"] >= 15
        ]
        if len(major) < 2:
            return None

        if len(major) > 2:
            by_green = sorted(major, key=lambda i: plan["phases"][i]["green"], reverse=True)
            second_green = plan["phases"][by_green[1]]["green"]
            third_green = plan["phases"][by_green[2]]["green"]
            if third_green > second_green * 0.80:
                return None
            major = sorted(by_green[:2])

        heading_bucket = self._cardinal_bucket(bearing_deg)
        same_axis = (heading_bucket % 4) == (plan["direction"] % 4)
        selected_index = major[0] if same_axis else major[1]

        timeline = 0
        selected_start = None
        selected = None
        for i, ph in enumerate(plan["phases"]):
            if i == selected_index:
                selected_start = timeline
                selected = ph
            timeline += ph["green"] + ph["yellow"] + ph["allred"]

        if selected is None or selected_start is None or timeline <= 0:
            return None

        cycle = timeline if abs(timeline - plan["cycle"]) <= 2 else plan["cycle"]
        if cycle <= 0 or selected_start >= cycle:
            return None

        pos = (now.hour * 3600 + now.minute * 60 + now.second - plan["offset"]) % cycle
        green_end = selected_start + selected["green"]
        yellow_end = green_end + selected["yellow"]

        if selected_start <= pos < green_end:
            state = "GREEN"
            remaining = max(1, green_end - pos)
        elif selected["yellow"] > 0 and green_end <= pos < yellow_end:
            state = "YELLOW"
            remaining = max(1, yellow_end - pos)
        else:
            state = "RED"
            if pos < selected_start:
                remaining = selected_start - pos
            else:
                remaining = cycle - pos + selected_start
            remaining = max(1, remaining)

        return {
            "state": state,
            "remaining_s": int(remaining),
            "plan_id": plan_id,
            "phaseorder": plan["phaseorder"],
        }

    def snapshot(self, latitude, longitude, bearing_deg, speed_mps=0.0,
                 accuracy_m=20.0, limit=3, preferred_ids=None):
        now = datetime.now(self.tz)
        speed_mps = max(0.0, float(speed_mps or 0.0))
        accuracy_m = max(5.0, float(accuracy_m or 20.0))
        limit = max(1, min(3, int(limit or 3)))

        preferred_ids = [str(x) for x in (preferred_ids or []) if str(x)]
        if preferred_ids:
            items = []
            for icid in preferred_ids[:limit]:
                s = self.intersections_by_id.get(icid)
                if not s:
                    continue
                estimate = self._estimate(icid, bearing_deg, now)
                item = {
                    "intersection_id": icid,
                    "name": s["name"],
                    "distance_m": round(self._distance_m(latitude, longitude, s["lat"], s["lon"]), 1),
                    "state": "UNKNOWN",
                    "remaining_s": None,
                }
                if estimate:
                    item.update(estimate)
                items.append(item)
            if items:
                return {
                    "ok": True,
                    "source": "MCP_REALTIME",
                    "server_time_ms": int(time.time() * 1000),
                    "items": items,
                }

        max_distance = 1800.0 if speed_mps >= 12.0 else 1200.0
        max_angle = 32.0 if speed_mps >= 12.0 else 45.0
        max_cross = max(55.0 if speed_mps >= 12.0 else 70.0, accuracy_m * 1.8)

        candidates = []
        for s in self.intersections:
            distance = self._distance_m(latitude, longitude, s["lat"], s["lon"])
            if distance < 10.0 or distance > max_distance:
                continue
            target_bearing = self._bearing_deg(latitude, longitude, s["lat"], s["lon"])
            relative = self._angle_delta(bearing_deg, target_bearing)
            rad = math.radians(relative)
            along = distance * math.cos(rad)
            cross = abs(distance * math.sin(rad))
            if along <= 8.0 or abs(relative) > max_angle or cross > max_cross:
                continue
            candidates.append((along, cross, distance, s))

        candidates.sort(key=lambda x: (x[0], x[1], x[2]))
        items = []
        seen = set()
        for along, cross, distance, s in candidates:
            key = " ".join(s["name"].lower().split())
            if key in seen:
                continue
            seen.add(key)
            estimate = self._estimate(s["id"], bearing_deg, now)
            item = {
                "intersection_id": s["id"],
                "name": s["name"],
                "distance_m": round(distance, 1),
                "state": "UNKNOWN",
                "remaining_s": None,
            }
            if estimate:
                item.update(estimate)
            items.append(item)
            if len(items) >= limit:
                break

        return {
            "ok": True,
            "source": "MCP_REALTIME",
            "server_time_ms": int(time.time() * 1000),
            "items": items,
        }


class State:
    def __init__(self, db_path: Path, token: str, hud_token: str, hud_data_dir: Path):
        self.db_path = db_path
        self.token = token
        self.hud_token = hud_token
        self.hud_model = HudModel(hud_data_dir)


class Handler(BaseHTTPRequestHandler):
    server_version = "SignalLoggerMCP/0.1"

    def _json(self, status, payload):
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _auth_role(self):
        header = self.headers.get("Authorization", "")
        bearer = header[7:] if header.startswith("Bearer ") else ""
        state = self.server.state
        if not state.token and not state.hud_token:
            return "admin"
        if bearer and bearer == state.token:
            return "logger"
        if bearer and state.hud_token and bearer == state.hud_token:
            return "hud"
        return None

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"ok": True, "service": "SignalLoggerMCP", "time_ms": int(time.time() * 1000)})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/mcp":
            self._json(404, {"error": "not found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length) or b"{}")
            role = self._auth_role()
            if role is None:
                self._json(401, {"error": "unauthorized"})
                return
            response = self.handle_mcp(request, role)
            tool_name = ""
            try:
                if request.get("method") == "tools/call":
                    tool_name = str((request.get("params") or {}).get("name") or "")
                structured = ((response.get("result") or {}).get("structuredContent") or {})
                if tool_name:
                    print(
                        "MCP_CALL"
                        f" tool={tool_name}"
                        f" ok={structured.get('ok')}"
                        f" accepted={structured.get('accepted_through_seq')}"
                        f" new_count={structured.get('new_count')}",
                        flush=True,
                    )
            except Exception:
                pass
            self._json(200, response)
        except Exception as exc:
            try:
                tool_name = str(((request.get("params") or {}).get("name") or "")) if isinstance(request, dict) else ""
            except Exception:
                tool_name = ""
            print(f"MCP_ERROR tool={tool_name} error={exc}", flush=True)
            req_id = None
            try:
                req_id = request.get("id")
            except Exception:
                pass
            self._json(200, {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32000, "message": str(exc)},
            })

    def handle_mcp(self, request, role):
        method = request.get("method")
        req_id = request.get("id")
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "SignalLoggerMCP", "version": "0.1.0"},
                },
            }
        if method == "notifications/initialized":
            return {"jsonrpc": "2.0", "id": req_id, "result": {}}
        if method == "tools/list":
            visible = TOOLS
            if role == "hud":
                visible = [t for t in TOOLS if t["name"] in {"signal.health", "signal.hud_snapshot"}]
            return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": visible}}
        if method != "tools/call":
            raise ValueError(f"unsupported MCP method: {method}")

        params = request.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}
        if role == "hud" and name not in {"signal.health", "signal.hud_snapshot"}:
            raise PermissionError("HUD token is limited to read-only HUD tools")
        result = self.call_tool(name, args)
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [{"type": "text", "text": json.dumps(result, separators=(",", ":"), ensure_ascii=False)}],
                "structuredContent": result,
                "isError": False,
            },
        }

    def call_tool(self, name, args):
        if name == "signal.health":
            return {"ok": True, "server_time_ms": int(time.time() * 1000)}
        if name == "signal.hud_snapshot":
            return self.server.state.hud_model.snapshot(
                float(args["latitude"]),
                float(args["longitude"]),
                float(args["bearing_deg"]),
                float(args.get("speed_mps", 0.0)),
                float(args.get("accuracy_m", 20.0)),
                int(args.get("limit", 3)),
                args.get("intersection_ids") or [],
            )
        if name == "signal.start_session":
            return self.start_session(args)
        if name == "signal.append_batch":
            return self.append_batch(args)
        if name == "signal.end_session":
            return self.end_session(args)
        if name == "signal.sync_status":
            return self.sync_status(args)
        raise ValueError(f"unknown tool: {name}")

    def start_session(self, args):
        session_id = str(args["session_id"])
        now = int(time.time() * 1000)
        payload = json.dumps(args, separators=(",", ":"), ensure_ascii=False)
        with DB_LOCK, db_connection(self.server.state.db_path) as con:
            con.execute(
                """
                INSERT INTO ingest_session(
                    session_id,started_at_utc_ms,state,device_install_id,device_model,
                    android_version,app_version,sensor_capabilities,start_payload_json,
                    server_first_seen_ms,server_last_seen_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(session_id) DO UPDATE SET
                    server_last_seen_ms=excluded.server_last_seen_ms,
                    start_payload_json=excluded.start_payload_json
                """,
                (
                    session_id,
                    args.get("started_at_utc_ms"),
                    "RECORDING",
                    args.get("device_install_id"),
                    args.get("device_model"),
                    args.get("android_version"),
                    args.get("app_version"),
                    args.get("sensor_capabilities"),
                    payload,
                    now,
                    now,
                ),
            )
        return {"ok": True, "session_id": session_id, "server_time_ms": now}

    def append_batch(self, args):
        session_id = str(args["session_id"])
        stream_type = str(args["stream_type"])
        samples = list(args.get("samples") or [])
        now = int(time.time() * 1000)
        accepted = int(args.get("first_seq", 0)) - 1
        inserted = 0
        with DB_LOCK, db_connection(self.server.state.db_path) as con:
            for sample in samples:
                seq = int(sample["seq"])
                device_utc = sample.get("fix_utc_ms", sample.get("utc_ms"))
                cur = con.execute(
                    "INSERT OR IGNORE INTO raw_sample(session_id,stream_type,seq,payload_json,device_utc_ms,server_received_ms) VALUES(?,?,?,?,?,?)",
                    (session_id, stream_type, seq, json.dumps(sample, separators=(",", ":"), ensure_ascii=False), device_utc, now),
                )
                if cur.rowcount > 0:
                    inserted += 1
                if seq > accepted:
                    accepted = seq
            con.execute(
                "UPDATE ingest_session SET server_last_seen_ms=? WHERE session_id=?",
                (now, session_id),
            )
        return {
            "ok": True,
            "session_id": session_id,
            "stream_type": stream_type,
            "accepted_through_seq": accepted,
            "received_count": len(samples),
            "new_count": inserted,
            "server_time_ms": now,
        }

    def end_session(self, args):
        session_id = str(args["session_id"])
        now = int(time.time() * 1000)
        payload = json.dumps(args, separators=(",", ":"), ensure_ascii=False)
        with DB_LOCK, db_connection(self.server.state.db_path) as con:
            con.execute(
                "UPDATE ingest_session SET ended_at_utc_ms=?, state=?, end_payload_json=?, server_last_seen_ms=? WHERE session_id=?",
                (args.get("ended_at_utc_ms"), args.get("state", "COMPLETED"), payload, now, session_id),
            )
        return {"ok": True, "session_id": session_id, "server_time_ms": now}

    def sync_status(self, args):
        session_id = str(args["session_id"])
        streams = {}
        with DB_LOCK, db_connection(self.server.state.db_path) as con:
            for stream_type, max_seq in con.execute(
                "SELECT stream_type, COALESCE(MAX(seq),0) FROM raw_sample WHERE session_id=? GROUP BY stream_type",
                (session_id,),
            ):
                streams[stream_type] = max_seq
        return {"ok": True, "session_id": session_id, "streams": streams, "server_time_ms": int(time.time() * 1000)}

    def log_message(self, fmt, *args):
        print("[%s] %s" % (self.log_date_time_string(), fmt % args), flush=True)


class Server(ThreadingHTTPServer):
    def __init__(self, address, handler, state):
        super().__init__(address, handler)
        self.state = state


def main():
    default_db = Path.home() / "Bryce AI Studio" / "SignalLoggerData" / "signal_ingest.sqlite"
    default_hud_data = Path.home() / "Bryce AI Studio" / "TaipeiSignalHUD" / "app" / "src" / "main" / "assets"
    parser = argparse.ArgumentParser(description="SignalLogger MCP receiver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", type=Path, default=default_db)
    parser.add_argument("--token", default=os.environ.get("SIGNALLOGGER_TOKEN", ""))
    parser.add_argument("--hud-token", default=os.environ.get("SIGNALHUD_TOKEN", ""))
    parser.add_argument("--hud-data-dir", type=Path, default=default_hud_data)
    args = parser.parse_args()

    init_db(args.db)
    state = State(args.db, args.token, args.hud_token, args.hud_data_dir)
    server = Server((args.host, args.port), Handler, state)
    print(f"SignalLogger MCP listening on http://{args.host}:{args.port}/mcp", flush=True)
    print(f"DB: {args.db}", flush=True)
    print("Auth: " + ("Bearer token required" if args.token else "disabled"), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
