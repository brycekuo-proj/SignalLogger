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
                "fix_utc_ms": {"type": "integer"},
                "request_utc_ms": {"type": "integer"},
            },
        },
    },
    {
        "name": "signal.consistency_events",
        "description": "Read recent HUD-vs-OPPO motion conflict records for calibration.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer"},
                "event_type": {"type": "string"}
            }
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
        con.execute("CREATE INDEX IF NOT EXISTS idx_raw_sample_device_time ON raw_sample(stream_type, device_utc_ms)")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_consistency_event (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT NOT NULL,
                time_bucket INTEGER NOT NULL,
                first_seen_ms INTEGER NOT NULL,
                last_seen_ms INTEGER NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 1,
                intersection_id TEXT NOT NULL,
                intersection_name TEXT,
                hud_state TEXT NOT NULL,
                hud_remaining_s INTEGER,
                a37_fix_utc_ms INTEGER,
                a37_latitude REAL,
                a37_longitude REAL,
                a37_speed_mps REAL,
                a37_bearing_deg REAL,
                oppo_session_id TEXT,
                oppo_seq INTEGER,
                oppo_fix_utc_ms INTEGER,
                oppo_latitude REAL,
                oppo_longitude REAL,
                oppo_speed_mps REAL,
                oppo_bearing_deg REAL,
                device_distance_m REAL,
                oppo_intersection_distance_m REAL,
                bearing_delta_deg REAL,
                reason TEXT,
                payload_json TEXT,
                UNIQUE(event_type, intersection_id, time_bucket)
            )
            """
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_signal_consistency_latest "
            "ON signal_consistency_event(last_seen_ms DESC)"
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS hud_observation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fix_utc_ms INTEGER NOT NULL,
                server_received_ms INTEGER NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                speed_mps REAL,
                bearing_deg REAL NOT NULL,
                accuracy_m REAL,
                intersection_id TEXT NOT NULL,
                intersection_name TEXT,
                hud_state TEXT NOT NULL,
                hud_remaining_s INTEGER,
                intersection_distance_m REAL,
                UNIQUE(fix_utc_ms, intersection_id)
            )
            """
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_hud_observation_time "
            "ON hud_observation(fix_utc_ms)"
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_timing_calibration (
                intersection_id TEXT PRIMARY KEY,
                phase_adjust_sec INTEGER NOT NULL,
                source TEXT,
                updated_at_ms INTEGER NOT NULL
            )
            """
        )
        con.execute(
            """
            INSERT INTO signal_timing_calibration(
                intersection_id,phase_adjust_sec,source,updated_at_ms
            ) VALUES('*',2,'field_report_fixed_2s_2026-09-19',?)
            ON CONFLICT(intersection_id) DO UPDATE SET
                phase_adjust_sec=excluded.phase_adjust_sec,
                source=excluded.source,
                updated_at_ms=excluded.updated_at_ms
            """,
            (int(time.time() * 1000),),
        )


class HudModel:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.intersections = []
        self.intersections_by_id = {}
        self.plans = {}
        self.schedules = {}
        self.phase_adjustments = {}
        self.tz = ZoneInfo("Asia/Taipei")
        self.xinsheng_exits = {
            "N": [
                {"lat": 25.048270, "intersection_id": "IKVJB", "label": "新生高架・長安出口"},
                {"lat": 25.058025, "intersection_id": "IN5IU", "label": "新生高架・民生出口"},
                {"lat": 25.076420, "intersection_id": "ISBID", "label": "新生高架・北安出口"},
                {"lat": 25.079859, "intersection_id": "ISVI9", "label": "新生高架・通河出口"},
            ],
            "S": [
                {"lat": 25.070747, "intersection_id": "IR8IP", "label": "新生高架・濱江出口"},
                {"lat": 25.054901, "intersection_id": "IMFIU", "label": "新生高架・長春出口"},
                {"lat": 25.048270, "intersection_id": "IKVJB", "label": "新生高架・長安出口"},
                {"lat": 25.043123, "intersection_id": "IJSJD", "label": "新生高架・忠孝出口"},
                {"lat": 25.040840, "intersection_id": "IJBJ9", "label": "新生高架・濟南出口"},
            ],
        }
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

        phase_adjust_sec = int(
            self.phase_adjustments.get(icid, self.phase_adjustments.get("*", 0))
        )
        pos = (
            now.hour * 3600
            + now.minute * 60
            + now.second
            + phase_adjust_sec
            - plan["offset"]
        ) % cycle
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
            "phase_adjust_sec": phase_adjust_sec,
        }

    def snapshot(self, latitude, longitude, bearing_deg, speed_mps=0.0,
                 accuracy_m=20.0, limit=3, preferred_ids=None):
        now = datetime.now(self.tz)
        speed_mps = max(0.0, float(speed_mps or 0.0))
        accuracy_m = max(5.0, float(accuracy_m or 20.0))
        limit = max(1, min(3, int(limit or 3)))

        max_distance = 1800.0 if speed_mps >= 12.0 else 1200.0
        max_angle = 32.0 if speed_mps >= 12.0 else 45.0
        max_cross = max(55.0 if speed_mps >= 12.0 else 70.0, accuracy_m * 1.8)

        # Preferred IDs come from the phone's previous corridor match. They are hints,
        # not authority: after a turn, GPS jump, or stale stop-state they may no longer
        # be in front of the vehicle. Revalidate and re-sort them against the current
        # fix before returning any signal rows.
        preferred_ids = [str(x) for x in (preferred_ids or []) if str(x)]
        if preferred_ids:
            preferred_candidates = []
            for icid in preferred_ids[:limit]:
                s = self.intersections_by_id.get(icid)
                if not s:
                    continue
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
                preferred_candidates.append((along, cross, distance, s))

            preferred_candidates.sort(key=lambda x: (x[0], x[1], x[2]))
            items = []
            for along, cross, distance, s in preferred_candidates[:limit]:
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
            if items:
                return {
                    "ok": True,
                    "source": "MCP_REALTIME",
                    "server_time_ms": int(time.time() * 1000),
                    "items": items,
                }

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
        init_db(db_path)
        self.db_path = db_path
        self.token = token
        self.hud_token = hud_token
        self.hud_model = HudModel(hud_data_dir)
        self.xinsheng_elevated_until_ms = 0
        self.xinsheng_elevated_direction = ""
        self._load_timing_calibration()

    def _load_timing_calibration(self):
        adjustments = {}
        with DB_LOCK, db_connection(self.db_path) as con:
            for intersection_id, adjust_sec in con.execute(
                "SELECT intersection_id, phase_adjust_sec FROM signal_timing_calibration"
            ):
                adjustments[str(intersection_id)] = int(adjust_sec)
        self.hud_model.phase_adjustments = adjustments

    def _travel_direction(self, bearing_deg):
        b = float(bearing_deg) % 360.0
        if b <= 45.0 or b >= 315.0:
            return "N"
        if 135.0 <= b <= 225.0:
            return "S"
        return ""

    def _recent_xinsheng_conflict_count(self, now_ms):
        with DB_LOCK, db_connection(self.db_path) as con:
            row = con.execute(
                """
                SELECT COUNT(*)
                FROM signal_consistency_event
                WHERE event_type='RED_WHILE_MOVING'
                  AND last_seen_ms >= ?
                  AND (
                    intersection_name LIKE '%新生北%'
                    OR intersection_id IN ('IR8IP','IQEIV','IPLIV','IP6IV','IN5IU','IMFIU')
                  )
                  AND COALESCE(oppo_speed_mps,0) >= 10.0
                """,
                (now_ms - 45000,),
            ).fetchone()
        return int((row or [0])[0] or 0)

    def _detect_xinsheng_elevated(self, args):
        lat = float(args["latitude"])
        lon = float(args["longitude"])
        speed = max(0.0, float(args.get("speed_mps", 0.0)))
        direction = self._travel_direction(args.get("bearing_deg", 0.0))
        now_ms = int(time.time() * 1000)

        in_corridor = (
            25.0390 <= lat <= 25.0815
            and 121.5220 <= lon <= 121.5348
            and direction in {"N", "S"}
        )
        if not in_corridor:
            return ""

        strong_speed = speed >= 15.0
        supported_speed = speed >= 10.0 and self._recent_xinsheng_conflict_count(now_ms) >= 1

        if strong_speed or supported_speed:
            self.xinsheng_elevated_until_ms = now_ms + 25000
            self.xinsheng_elevated_direction = direction
            return direction

        if (
            now_ms <= self.xinsheng_elevated_until_ms
            and self.xinsheng_elevated_direction == direction
        ):
            return direction

        return ""

    def _xinsheng_ramp_snapshot(self, args, direction):
        lat = float(args["latitude"])
        lon = float(args["longitude"])
        bearing = float(args["bearing_deg"])
        now = datetime.now(self.hud_model.tz)

        exits = self.hud_model.xinsheng_exits.get(direction, [])
        target = None
        if direction == "N":
            for item in exits:
                if item["lat"] > lat + 0.00020:
                    target = item
                    break
        elif direction == "S":
            for item in exits:
                if item["lat"] < lat - 0.00020:
                    target = item
                    break

        if target is None:
            return None

        icid = target["intersection_id"]
        intersection = self.hud_model.intersections_by_id.get(icid)
        if intersection is None:
            return None

        estimate = self.hud_model._estimate(icid, bearing, now)
        item = {
            "intersection_id": icid,
            "name": target["label"],
            "distance_m": round(
                self.hud_model._distance_m(
                    lat, lon, intersection["lat"], intersection["lon"]
                ),
                1,
            ),
            "state": "UNKNOWN",
            "remaining_s": None,
            "road_mode": "XINSHENG_ELEVATED",
            "exit_name": target["label"],
        }
        if estimate:
            item.update(estimate)

        return {
            "ok": True,
            "source": "MCP_XINSHENG_ELEVATED",
            "server_time_ms": int(time.time() * 1000),
            "road_mode": "XINSHENG_ELEVATED",
            "direction": direction,
            "items": [item],
        }

    def hud_snapshot(self, args):
        direction = self._detect_xinsheng_elevated(args)
        snapshot = self._xinsheng_ramp_snapshot(args, direction) if direction else None
        if snapshot is None:
            snapshot = self.hud_model.snapshot(
                float(args["latitude"]),
                float(args["longitude"]),
                float(args["bearing_deg"]),
                float(args.get("speed_mps", 0.0)),
                float(args.get("accuracy_m", 20.0)),
                int(args.get("limit", 3)),
                args.get("intersection_ids") or [],
            )
        self.store_hud_observation(args, snapshot)
        snapshot["oppo_comparison"] = self.compare_hud_with_oppo(args, snapshot)
        return snapshot

    def store_hud_observation(self, args, snapshot):
        items = snapshot.get("items") or []
        if not items:
            return
        primary = items[0]
        icid = str(primary.get("intersection_id") or "")
        if not icid:
            return

        now_ms = int(time.time() * 1000)
        ref_ms = int(args.get("fix_utc_ms") or args.get("request_utc_ms") or now_ms)
        with DB_LOCK, db_connection(self.db_path) as con:
            con.execute(
                """
                INSERT INTO hud_observation(
                    fix_utc_ms,server_received_ms,latitude,longitude,speed_mps,
                    bearing_deg,accuracy_m,intersection_id,intersection_name,
                    hud_state,hud_remaining_s,intersection_distance_m
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(fix_utc_ms,intersection_id) DO UPDATE SET
                    server_received_ms=excluded.server_received_ms,
                    latitude=excluded.latitude,
                    longitude=excluded.longitude,
                    speed_mps=excluded.speed_mps,
                    bearing_deg=excluded.bearing_deg,
                    accuracy_m=excluded.accuracy_m,
                    intersection_name=excluded.intersection_name,
                    hud_state=excluded.hud_state,
                    hud_remaining_s=excluded.hud_remaining_s,
                    intersection_distance_m=excluded.intersection_distance_m
                """,
                (
                    ref_ms,
                    now_ms,
                    float(args["latitude"]),
                    float(args["longitude"]),
                    max(0.0, float(args.get("speed_mps", 0.0))),
                    float(args["bearing_deg"]) % 360.0,
                    max(5.0, float(args.get("accuracy_m", 20.0))),
                    icid,
                    primary.get("name"),
                    primary.get("state") or "UNKNOWN",
                    primary.get("remaining_s"),
                    primary.get("distance_m"),
                ),
            )

    def reconcile_oppo_location_batch(self, samples):
        moving_times = []
        for sample in samples or []:
            try:
                fix_ms = int(sample.get("fix_utc_ms") or sample.get("utc_ms") or 0)
                speed = float(sample.get("speed_mps") or 0.0)
                if fix_ms > 0 and speed >= 2.0:
                    moving_times.append(fix_ms)
            except Exception:
                continue
        if not moving_times:
            return 0

        start_ms = min(moving_times) - 4000
        end_ms = max(moving_times) + 4000
        with DB_LOCK, db_connection(self.db_path) as con:
            rows = list(con.execute(
                """
                SELECT fix_utc_ms,latitude,longitude,speed_mps,bearing_deg,accuracy_m,
                       intersection_id,intersection_name,hud_state,hud_remaining_s,
                       intersection_distance_m
                FROM hud_observation
                WHERE fix_utc_ms BETWEEN ? AND ?
                  AND hud_state='RED'
                ORDER BY fix_utc_ms
                """,
                (start_ms, end_ms),
            ))

        # One check per 5-second/intersection bucket is enough; the event table
        # itself also merges repeated evidence into that bucket.
        selected = {}
        for row in rows:
            key = (row[6], int(row[0]) // 5000)
            selected.setdefault(key, row)

        recorded = 0
        for row in selected.values():
            args = {
                "fix_utc_ms": int(row[0]),
                "latitude": float(row[1]),
                "longitude": float(row[2]),
                "speed_mps": float(row[3] or 0.0),
                "bearing_deg": float(row[4]),
                "accuracy_m": float(row[5] or 20.0),
            }
            snapshot = {
                "items": [{
                    "intersection_id": row[6],
                    "name": row[7],
                    "state": row[8],
                    "remaining_s": row[9],
                    "distance_m": row[10],
                }]
            }
            result = self.compare_hud_with_oppo(args, snapshot)
            if result.get("recorded"):
                recorded += 1
        return recorded

    def compare_hud_with_oppo(self, args, snapshot):
        items = snapshot.get("items") or []
        if not items:
            return {"status": "NO_SIGNAL", "recorded": False}

        primary = items[0]
        icid = str(primary.get("intersection_id") or "")
        intersection = self.hud_model.intersections_by_id.get(icid)
        if not intersection:
            return {"status": "NO_INTERSECTION", "recorded": False}

        now_ms = int(time.time() * 1000)
        ref_ms = int(args.get("fix_utc_ms") or args.get("request_utc_ms") or now_ms)
        a37_lat = float(args["latitude"])
        a37_lon = float(args["longitude"])
        a37_bearing = float(args["bearing_deg"]) % 360.0
        a37_speed = max(0.0, float(args.get("speed_mps", 0.0)))
        a37_accuracy = max(5.0, float(args.get("accuracy_m", 20.0)))

        with DB_LOCK, db_connection(self.db_path) as con:
            rows = list(con.execute(
                """
                SELECT session_id, seq, payload_json, device_utc_ms
                FROM raw_sample
                WHERE stream_type='location'
                  AND device_utc_ms BETWEEN ? AND ?
                ORDER BY ABS(device_utc_ms - ?)
                LIMIT 8
                """,
                (ref_ms - 4000, ref_ms + 4000, ref_ms),
            ))

        if not rows:
            return {"status": "NO_OPPO_SAMPLE", "recorded": False}

        valid = []
        nearest = None
        nearest_delta = None
        for session_id, seq, payload_json, device_utc_ms in rows:
            try:
                p = json.loads(payload_json)
                oppo_lat = float(p["latitude"])
                oppo_lon = float(p["longitude"])
                oppo_speed = max(0.0, float(p.get("speed_mps") or 0.0))
                oppo_bearing = float(p.get("bearing_deg") or 0.0) % 360.0
                oppo_accuracy = max(5.0, float(p.get("horizontal_accuracy_m") or 20.0))
                device_distance = self.hud_model._distance_m(
                    a37_lat, a37_lon, oppo_lat, oppo_lon
                )
                heading_delta = abs(self.hud_model._angle_delta(a37_bearing, oppo_bearing))
                max_device_distance = max(
                    35.0, min(70.0, a37_accuracy + oppo_accuracy + 15.0)
                )
                time_delta = abs(int(device_utc_ms or 0) - ref_ms)
                sample = {
                    "session_id": session_id,
                    "seq": int(seq),
                    "fix_utc_ms": int(device_utc_ms or 0),
                    "latitude": oppo_lat,
                    "longitude": oppo_lon,
                    "speed_mps": oppo_speed,
                    "bearing_deg": oppo_bearing,
                    "accuracy_m": oppo_accuracy,
                    "device_distance_m": device_distance,
                    "bearing_delta_deg": heading_delta,
                    "time_delta_ms": time_delta,
                }
                if nearest is None or time_delta < nearest_delta:
                    nearest = sample
                    nearest_delta = time_delta
                if (
                    time_delta <= 4000
                    and oppo_accuracy <= 35.0
                    and device_distance <= max_device_distance
                    and (oppo_speed < 1.5 or heading_delta <= 35.0)
                ):
                    valid.append(sample)
            except Exception:
                continue

        if not valid:
            return {
                "status": "OPPO_NOT_COLOCATED",
                "recorded": False,
                "nearest_time_delta_ms": nearest_delta,
                "nearest_device_distance_m": round(nearest["device_distance_m"], 1) if nearest else None,
            }

        valid.sort(key=lambda s: s["fix_utc_ms"])
        speeds = sorted(s["speed_mps"] for s in valid)
        median_speed = speeds[len(speeds) // 2]
        moving_samples = [s for s in valid if s["speed_mps"] >= 2.0]
        best = min(valid, key=lambda s: s["time_delta_ms"])

        oppo_intersection_distance = self.hud_model._distance_m(
            best["latitude"], best["longitude"], intersection["lat"], intersection["lon"]
        )
        a37_intersection_distance = float(primary.get("distance_m") or 99999.0)
        same_approach = (
            best["bearing_delta_deg"] <= 35.0
            and best["device_distance_m"] <= max(
                35.0, min(70.0, a37_accuracy + best["accuracy_m"] + 15.0)
            )
        )
        near_signal = (
            a37_intersection_distance <= 55.0
            and oppo_intersection_distance <= 55.0
        )
        sustained_moving = len(moving_samples) >= 2 and median_speed >= 2.0

        comparison = {
            "status": "NO_CONFLICT",
            "recorded": False,
            "oppo_session_id": best["session_id"],
            "oppo_seq": best["seq"],
            "time_delta_ms": best["time_delta_ms"],
            "device_distance_m": round(best["device_distance_m"], 1),
            "bearing_delta_deg": round(best["bearing_delta_deg"], 1),
            "oppo_speed_mps": round(median_speed, 2),
            "oppo_intersection_distance_m": round(oppo_intersection_distance, 1),
        }

        if (
            primary.get("state") == "RED"
            and primary.get("remaining_s") is not None
            and same_approach
            and near_signal
            and sustained_moving
        ):
            event_type = "RED_WHILE_MOVING"
            reason = (
                "HUD predicted RED while colocated OPPO GPS showed sustained movement "
                "near the same intersection/approach"
            )
            event_payload = {
                "a37": {
                    "fix_utc_ms": ref_ms,
                    "latitude": a37_lat,
                    "longitude": a37_lon,
                    "speed_mps": a37_speed,
                    "bearing_deg": a37_bearing,
                    "accuracy_m": a37_accuracy,
                },
                "hud": primary,
                "oppo": best,
                "valid_oppo_sample_count": len(valid),
                "moving_oppo_sample_count": len(moving_samples),
                "median_oppo_speed_mps": median_speed,
            }
            bucket = ref_ms // 5000
            with DB_LOCK, db_connection(self.db_path) as con:
                con.execute(
                    """
                    INSERT INTO signal_consistency_event(
                        event_type,time_bucket,first_seen_ms,last_seen_ms,sample_count,
                        intersection_id,intersection_name,hud_state,hud_remaining_s,
                        a37_fix_utc_ms,a37_latitude,a37_longitude,a37_speed_mps,a37_bearing_deg,
                        oppo_session_id,oppo_seq,oppo_fix_utc_ms,oppo_latitude,oppo_longitude,
                        oppo_speed_mps,oppo_bearing_deg,device_distance_m,
                        oppo_intersection_distance_m,bearing_delta_deg,reason,payload_json
                    ) VALUES(?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(event_type,intersection_id,time_bucket) DO UPDATE SET
                        last_seen_ms=excluded.last_seen_ms,
                        sample_count=signal_consistency_event.sample_count+1,
                        hud_remaining_s=excluded.hud_remaining_s,
                        a37_fix_utc_ms=excluded.a37_fix_utc_ms,
                        a37_latitude=excluded.a37_latitude,
                        a37_longitude=excluded.a37_longitude,
                        a37_speed_mps=excluded.a37_speed_mps,
                        a37_bearing_deg=excluded.a37_bearing_deg,
                        oppo_session_id=excluded.oppo_session_id,
                        oppo_seq=excluded.oppo_seq,
                        oppo_fix_utc_ms=excluded.oppo_fix_utc_ms,
                        oppo_latitude=excluded.oppo_latitude,
                        oppo_longitude=excluded.oppo_longitude,
                        oppo_speed_mps=excluded.oppo_speed_mps,
                        oppo_bearing_deg=excluded.oppo_bearing_deg,
                        device_distance_m=excluded.device_distance_m,
                        oppo_intersection_distance_m=excluded.oppo_intersection_distance_m,
                        bearing_delta_deg=excluded.bearing_delta_deg,
                        payload_json=excluded.payload_json
                    """,
                    (
                        event_type, bucket, now_ms, now_ms,
                        icid, primary.get("name"), primary.get("state"),
                        int(primary.get("remaining_s") or 0),
                        ref_ms, a37_lat, a37_lon, a37_speed, a37_bearing,
                        best["session_id"], best["seq"], best["fix_utc_ms"],
                        best["latitude"], best["longitude"], median_speed,
                        best["bearing_deg"], best["device_distance_m"],
                        oppo_intersection_distance, best["bearing_delta_deg"],
                        reason,
                        json.dumps(event_payload, separators=(",", ":"), ensure_ascii=False),
                    ),
                )
            comparison["status"] = "CONFLICT_RECORDED"
            comparison["recorded"] = True
            comparison["event_type"] = event_type

        return comparison

    def consistency_events(self, limit=50, event_type=""):
        limit = max(1, min(200, int(limit or 50)))
        query = """
            SELECT id,event_type,first_seen_ms,last_seen_ms,sample_count,
                   intersection_id,intersection_name,hud_state,hud_remaining_s,
                   oppo_speed_mps,device_distance_m,oppo_intersection_distance_m,
                   bearing_delta_deg,reason
            FROM signal_consistency_event
        """
        params = []
        if event_type:
            query += " WHERE event_type=?"
            params.append(str(event_type))
        query += " ORDER BY last_seen_ms DESC LIMIT ?"
        params.append(limit)

        events = []
        with DB_LOCK, db_connection(self.db_path) as con:
            for row in con.execute(query, params):
                events.append({
                    "id": row[0],
                    "event_type": row[1],
                    "first_seen_ms": row[2],
                    "last_seen_ms": row[3],
                    "sample_count": row[4],
                    "intersection_id": row[5],
                    "intersection_name": row[6],
                    "hud_state": row[7],
                    "hud_remaining_s": row[8],
                    "oppo_speed_mps": row[9],
                    "device_distance_m": row[10],
                    "oppo_intersection_distance_m": row[11],
                    "bearing_delta_deg": row[12],
                    "reason": row[13],
                })
        return {"ok": True, "events": events, "count": len(events)}


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

    def _send_file(self, path, download_name):
        file_path = Path(path)
        if not file_path.is_file():
            self._json(404, {"error": "file not found"})
            return
        size = file_path.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", "application/vnd.android.package-archive")
        self.send_header("Content-Length", str(size))
        self.send_header("Content-Disposition", f'attachment; filename="{download_name}"')
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        with file_path.open("rb") as src:
            while True:
                chunk = src.read(64 * 1024)
                if not chunk:
                    break
                self.wfile.write(chunk)

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"ok": True, "service": "SignalLoggerMCP", "time_ms": int(time.time() * 1000)})
            return
        if self.path == "/download/TaipeiSignalHUD.apk":
            self._send_file(
                "/Users/user/Bryce AI Studio/TaipeiSignalHUD/app/build/outputs/apk/debug/TaipeiSignalHUD-v0.0.9-roadtest-debug.apk",
                "TaipeiSignalHUD-v0.0.9-roadtest-debug.apk",
            )
            return
        if self.path == "/download/SignalLogger.apk":
            self._send_file(
                "/Users/user/Bryce AI Studio/SignalLogger/releases/SignalLogger-v0.1.3-debug.apk",
                "SignalLogger-v0.1.3-debug.apk",
            )
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
            return self.server.state.hud_snapshot(args)
        if name == "signal.consistency_events":
            return self.server.state.consistency_events(
                int(args.get("limit", 50)),
                str(args.get("event_type") or ""),
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
        new_samples = []
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
                    new_samples.append(sample)
                if seq > accepted:
                    accepted = seq
            con.execute(
                "UPDATE ingest_session SET server_last_seen_ms=? WHERE session_id=?",
                (now, session_id),
            )

        consistency_recorded = 0
        if stream_type == "location" and new_samples:
            consistency_recorded = self.server.state.reconcile_oppo_location_batch(new_samples)

        return {
            "ok": True,
            "session_id": session_id,
            "stream_type": stream_type,
            "accepted_through_seq": accepted,
            "received_count": len(samples),
            "new_count": inserted,
            "consistency_events_recorded": consistency_recorded,
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
