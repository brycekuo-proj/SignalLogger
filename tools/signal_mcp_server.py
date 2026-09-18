#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DB_LOCK = threading.Lock()

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
]


def init_db(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as con:
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


class State:
    def __init__(self, db_path: Path, token: str):
        self.db_path = db_path
        self.token = token


class Handler(BaseHTTPRequestHandler):
    server_version = "SignalLoggerMCP/0.1"

    def _json(self, status, payload):
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self):
        token = self.server.state.token
        if not token:
            return True
        return self.headers.get("Authorization", "") == f"Bearer {token}"

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {"ok": True, "service": "SignalLoggerMCP", "time_ms": int(time.time() * 1000)})
            return
        self._json(404, {"error": "not found"})

    def do_POST(self):
        if self.path != "/mcp":
            self._json(404, {"error": "not found"})
            return
        if not self._authorized():
            self._json(401, {"error": "unauthorized"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            request = json.loads(self.rfile.read(length) or b"{}")
            response = self.handle_mcp(request)
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

    def handle_mcp(self, request):
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
            return {"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}}
        if method != "tools/call":
            raise ValueError(f"unsupported MCP method: {method}")

        params = request.get("params") or {}
        name = params.get("name")
        args = params.get("arguments") or {}
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
        with DB_LOCK, sqlite3.connect(self.server.state.db_path) as con:
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
        with DB_LOCK, sqlite3.connect(self.server.state.db_path) as con:
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
        with DB_LOCK, sqlite3.connect(self.server.state.db_path) as con:
            con.execute(
                "UPDATE ingest_session SET ended_at_utc_ms=?, state=?, end_payload_json=?, server_last_seen_ms=? WHERE session_id=?",
                (args.get("ended_at_utc_ms"), args.get("state", "COMPLETED"), payload, now, session_id),
            )
        return {"ok": True, "session_id": session_id, "server_time_ms": now}

    def sync_status(self, args):
        session_id = str(args["session_id"])
        streams = {}
        with DB_LOCK, sqlite3.connect(self.server.state.db_path) as con:
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
    parser = argparse.ArgumentParser(description="SignalLogger MCP receiver")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--db", type=Path, default=default_db)
    parser.add_argument("--token", default=os.environ.get("SIGNALLOGGER_TOKEN", ""))
    args = parser.parse_args()

    init_db(args.db)
    state = State(args.db, args.token)
    server = Server((args.host, args.port), Handler, state)
    print(f"SignalLogger MCP listening on http://{args.host}:{args.port}/mcp", flush=True)
    print(f"DB: {args.db}", flush=True)
    print("Auth: " + ("Bearer token required" if args.token else "disabled"), flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
