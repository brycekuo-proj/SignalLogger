package com.bryce.signallogger;

import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

final class McpUploader {
    private final SignalDatabase db;
    private final String endpoint;
    private final String token;

    McpUploader(SignalDatabase db, String endpoint, String token) {
        this.db = db;
        this.endpoint = endpoint == null ? "" : endpoint.trim();
        this.token = token == null ? "" : token.trim();
    }

    boolean isConfigured() {
        return endpoint.startsWith("http://") || endpoint.startsWith("https://");
    }

    SyncResult syncOnce(String sessionId) {
        if (!isConfigured()) return new SyncResult(false, "NOT CONFIGURED", db.pendingCount(sessionId));
        try {
            SignalDatabase.SessionRecord start = db.pendingSessionStart(sessionId);
            if (start != null) {
                JSONObject result = callTool("signal.start_session", start.payload);
                if (result != null) db.markSessionStartSynced(sessionId);
            }

            syncBatch("location_sample", "location", sessionId, 120);
            syncBatch("gnss_snapshot", "gnss", sessionId, 60);
            syncBatch("sensor_sample", "sensor", sessionId, 300);

            SignalDatabase.SessionRecord end = db.pendingSessionEnd(sessionId);
            if (end != null) {
                JSONObject result = callTool("signal.end_session", end.payload);
                if (result != null) db.markSessionEndSynced(sessionId);
            }

            long pending = db.pendingCount(sessionId);
            return new SyncResult(true, "ONLINE", pending);
        } catch (Exception e) {
            return new SyncResult(false, "OFFLINE: " + shortMessage(e), db.pendingCount(sessionId));
        }
    }

    private void syncBatch(String table, String streamType, String sessionId, int limit) throws Exception {
        SignalDatabase.Batch batch = db.pendingBatch(table, streamType, sessionId, limit);
        if (batch == null) return;

        JSONObject args = new JSONObject();
        args.put("session_id", batch.sessionId);
        args.put("stream_type", batch.streamType);
        args.put("first_seq", batch.firstSeq);
        args.put("last_seq", batch.lastSeq);
        args.put("samples", batch.samples);

        JSONObject result = callTool("signal.append_batch", args);
        long accepted = result.optLong("accepted_through_seq", -1L);
        if (accepted < batch.firstSeq) {
            throw new IllegalStateException("Invalid ACK");
        }
        db.markBatchSynced(table, sessionId, accepted);
    }

    private JSONObject callTool(String toolName, JSONObject args) throws Exception {
        JSONObject params = new JSONObject();
        params.put("name", toolName);
        params.put("arguments", args);

        JSONObject request = new JSONObject();
        request.put("jsonrpc", "2.0");
        request.put("id", UUID.randomUUID().toString());
        request.put("method", "tools/call");
        request.put("params", params);

        byte[] payload = request.toString().getBytes(StandardCharsets.UTF_8);
        HttpURLConnection connection = (HttpURLConnection) new URL(endpoint).openConnection();
        connection.setRequestMethod("POST");
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(7000);
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("MCP-Protocol-Version", "2025-06-18");
        if (!token.isEmpty()) connection.setRequestProperty("Authorization", "Bearer " + token);

        try (OutputStream out = connection.getOutputStream()) {
            out.write(payload);
        }

        int code = connection.getResponseCode();
        InputStream in = code >= 200 && code < 300 ? connection.getInputStream() : connection.getErrorStream();
        String body = readAll(in);
        if (code < 200 || code >= 300) throw new IllegalStateException("HTTP " + code + " " + body);

        JSONObject response = new JSONObject(body);
        if (response.has("error")) throw new IllegalStateException(response.getJSONObject("error").optString("message", "MCP error"));
        JSONObject result = response.optJSONObject("result");
        if (result == null) throw new IllegalStateException("Missing MCP result");
        JSONObject structured = result.optJSONObject("structuredContent");
        if (structured != null) return structured;

        // Compatibility with simple MCP servers that return JSON text in content[0].text.
        if (result.optJSONArray("content") != null && result.optJSONArray("content").length() > 0) {
            String text = result.optJSONArray("content").optJSONObject(0).optString("text", "{}");
            return new JSONObject(text);
        }
        return result;
    }

    private static String readAll(InputStream in) throws Exception {
        if (in == null) return "";
        StringBuilder sb = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) sb.append(line);
        }
        return sb.toString();
    }

    private static String shortMessage(Exception e) {
        String s = e.getMessage();
        if (s == null || s.trim().isEmpty()) s = e.getClass().getSimpleName();
        if (s.length() > 80) s = s.substring(0, 80);
        return s;
    }

    static final class SyncResult {
        final boolean online;
        final String status;
        final long pending;

        SyncResult(boolean online, String status, long pending) {
            this.online = online;
            this.status = status;
            this.pending = pending;
        }
    }
}
