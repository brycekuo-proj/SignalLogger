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

    private long lastLocationAck = 0L;
    private long lastGnssAck = 0L;
    private long lastSensorAck = 0L;
    private long lastSuccessAtMs = 0L;

    McpUploader(SignalDatabase db, String endpoint, String token) {
        this.db = db;
        this.endpoint = endpoint == null ? "" : endpoint.trim();
        this.token = token == null ? "" : token.trim();
    }

    boolean isConfigured() {
        return endpoint.startsWith("http://") || endpoint.startsWith("https://");
    }

    SyncResult syncOnce(String sessionId) {
        if (!isConfigured()) {
            return new SyncResult(false, "NOT CONFIGURED", db.pendingCount(sessionId),
                    ackSummary(), lastSuccessAtMs, "MCP endpoint is not configured");
        }

        try {
            SignalDatabase.SessionRecord start = db.pendingSessionStart(sessionId);
            if (start != null) {
                callTool("signal.start_session", start.payload);
                db.markSessionStartSynced(sessionId);
            }

            // Reconcile with the Mac first. If a previous ACK was lost after the Mac
            // already committed the batch, this prevents the phone from retrying the
            // same rows forever.
            reconcileServerState(sessionId);

            // Drain more than one batch per pass so a temporary outage can catch up.
            drainStream("location_sample", "location", sessionId, 120, 4);
            drainStream("gnss_snapshot", "gnss", sessionId, 60, 4);
            drainStream("sensor_sample", "sensor", sessionId, 300, 4);

            SignalDatabase.SessionRecord end = db.pendingSessionEnd(sessionId);
            if (end != null) {
                callTool("signal.end_session", end.payload);
                db.markSessionEndSynced(sessionId);
            }

            long pending = db.pendingCount(sessionId);
            lastSuccessAtMs = System.currentTimeMillis();
            return new SyncResult(true, "ONLINE · ACK OK", pending,
                    ackSummary(), lastSuccessAtMs, "");
        } catch (Exception e) {
            return new SyncResult(false, "SYNC ERROR", db.pendingCount(sessionId),
                    ackSummary(), lastSuccessAtMs, shortMessage(e));
        }
    }

    private void reconcileServerState(String sessionId) throws Exception {
        JSONObject args = new JSONObject();
        args.put("session_id", sessionId);
        JSONObject result = callTool("signal.sync_status", args);
        JSONObject streams = result.optJSONObject("streams");
        if (streams == null) return;

        long location = Math.max(0L, streams.optLong("location", 0L));
        long gnss = Math.max(0L, streams.optLong("gnss", 0L));
        long sensor = Math.max(0L, streams.optLong("sensor", 0L));

        if (location > 0L) db.markBatchSynced("location_sample", sessionId, location);
        if (gnss > 0L) db.markBatchSynced("gnss_snapshot", sessionId, gnss);
        if (sensor > 0L) db.markBatchSynced("sensor_sample", sessionId, sensor);

        lastLocationAck = Math.max(lastLocationAck, location);
        lastGnssAck = Math.max(lastGnssAck, gnss);
        lastSensorAck = Math.max(lastSensorAck, sensor);
    }

    private void drainStream(String table, String streamType, String sessionId,
                             int limit, int maxBatches) throws Exception {
        for (int i = 0; i < maxBatches; i++) {
            SignalDatabase.Batch batch = db.pendingBatch(table, streamType, sessionId, limit);
            if (batch == null) return;
            long accepted = syncBatch(table, streamType, sessionId, batch);
            if (accepted < batch.lastSeq) {
                // Do not spin on a partial ACK. The next scheduled pass will reconcile
                // against signal.sync_status and continue safely.
                return;
            }
        }
    }

    private long syncBatch(String table, String streamType, String sessionId,
                           SignalDatabase.Batch batch) throws Exception {
        JSONObject args = new JSONObject();
        args.put("session_id", batch.sessionId);
        args.put("stream_type", batch.streamType);
        args.put("first_seq", batch.firstSeq);
        args.put("last_seq", batch.lastSeq);
        args.put("samples", batch.samples);

        JSONObject result = callTool("signal.append_batch", args);
        long accepted = result.optLong("accepted_through_seq", -1L);
        if (accepted < batch.firstSeq) {
            throw new IllegalStateException("Invalid ACK for " + streamType +
                    ": " + accepted + " < " + batch.firstSeq);
        }

        long safeAccepted = Math.min(accepted, batch.lastSeq);
        db.markBatchSynced(table, sessionId, safeAccepted);
        setAck(streamType, safeAccepted);
        return safeAccepted;
    }

    private void setAck(String streamType, long accepted) {
        if ("location".equals(streamType)) {
            lastLocationAck = Math.max(lastLocationAck, accepted);
        } else if ("gnss".equals(streamType)) {
            lastGnssAck = Math.max(lastGnssAck, accepted);
        } else if ("sensor".equals(streamType)) {
            lastSensorAck = Math.max(lastSensorAck, accepted);
        }
    }

    private String ackSummary() {
        return "GPS " + lastLocationAck + " · GNSS " + lastGnssAck + " · Sensor " + lastSensorAck;
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
        connection.setReadTimeout(10000);
        connection.setDoOutput(true);
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setRequestProperty("Accept", "application/json");
        connection.setRequestProperty("MCP-Protocol-Version", "2025-06-18");
        if (!token.isEmpty()) connection.setRequestProperty("Authorization", "Bearer " + token);

        try {
            try (OutputStream out = connection.getOutputStream()) {
                out.write(payload);
            }

            int code = connection.getResponseCode();
            InputStream in = code >= 200 && code < 300
                    ? connection.getInputStream() : connection.getErrorStream();
            String body = readAll(in);
            if (code < 200 || code >= 300) {
                throw new IllegalStateException("HTTP " + code + " " + body);
            }

            JSONObject response = new JSONObject(body);
            if (response.has("error")) {
                throw new IllegalStateException(response.getJSONObject("error")
                        .optString("message", "MCP error"));
            }

            JSONObject result = response.optJSONObject("result");
            if (result == null) throw new IllegalStateException("Missing MCP result");
            if (result.optBoolean("isError", false)) {
                throw new IllegalStateException("MCP tool returned isError=true");
            }

            JSONObject structured = result.optJSONObject("structuredContent");
            if (structured != null) {
                requireOk(toolName, structured);
                return structured;
            }

            // Compatibility with MCP servers that return JSON text in content[0].text.
            if (result.optJSONArray("content") != null && result.optJSONArray("content").length() > 0) {
                String text = result.optJSONArray("content").optJSONObject(0)
                        .optString("text", "{}");
                JSONObject parsed = new JSONObject(text);
                requireOk(toolName, parsed);
                return parsed;
            }

            // Some simple tools may return their payload directly as result.
            requireOk(toolName, result);
            return result;
        } finally {
            connection.disconnect();
        }
    }

    private static void requireOk(String toolName, JSONObject payload) {
        if (payload.has("ok") && !payload.optBoolean("ok", false)) {
            String message = payload.optString("error", payload.optString("message", "ok=false"));
            throw new IllegalStateException(toolName + ": " + message);
        }
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
        if (s.length() > 120) s = s.substring(0, 120);
        return s;
    }

    static final class SyncResult {
        final boolean online;
        final String status;
        final long pending;
        final String ackSummary;
        final long lastSuccessAtMs;
        final String error;

        SyncResult(boolean online, String status, long pending,
                   String ackSummary, long lastSuccessAtMs, String error) {
            this.online = online;
            this.status = status;
            this.pending = pending;
            this.ackSummary = ackSummary;
            this.lastSuccessAtMs = lastSuccessAtMs;
            this.error = error;
        }
    }
}
