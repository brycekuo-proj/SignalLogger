package com.bryce.signallogger;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.location.Location;
import android.os.Build;

import org.json.JSONArray;
import org.json.JSONObject;

final class SignalDatabase extends SQLiteOpenHelper {
    static final String DB_NAME = "signal_logger.db";
    private static final int DB_VERSION = 1;

    SignalDatabase(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
        setWriteAheadLoggingEnabled(true);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL("CREATE TABLE session (" +
                "session_id TEXT PRIMARY KEY," +
                "device_install_id TEXT NOT NULL," +
                "started_at_utc_ms INTEGER NOT NULL," +
                "ended_at_utc_ms INTEGER," +
                "started_elapsed_ns INTEGER NOT NULL," +
                "ended_elapsed_ns INTEGER," +
                "app_version TEXT NOT NULL," +
                "device_model TEXT," +
                "android_version TEXT," +
                "sensor_capabilities TEXT," +
                "state TEXT NOT NULL," +
                "start_synced INTEGER NOT NULL DEFAULT 0," +
                "end_synced INTEGER NOT NULL DEFAULT 0" +
                ")");

        db.execSQL("CREATE TABLE location_sample (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "session_id TEXT NOT NULL," +
                "seq INTEGER NOT NULL," +
                "fix_utc_ms INTEGER NOT NULL," +
                "elapsed_realtime_ns INTEGER NOT NULL," +
                "provider TEXT," +
                "latitude REAL NOT NULL," +
                "longitude REAL NOT NULL," +
                "horizontal_accuracy_m REAL," +
                "altitude_wgs84_m REAL," +
                "vertical_accuracy_m REAL," +
                "speed_mps REAL," +
                "speed_accuracy_mps REAL," +
                "bearing_deg REAL," +
                "bearing_accuracy_deg REAL," +
                "is_mock INTEGER NOT NULL DEFAULT 0," +
                "synced INTEGER NOT NULL DEFAULT 0," +
                "UNIQUE(session_id, seq)" +
                ")");

        db.execSQL("CREATE TABLE gnss_snapshot (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "session_id TEXT NOT NULL," +
                "seq INTEGER NOT NULL," +
                "utc_ms INTEGER NOT NULL," +
                "elapsed_realtime_ns INTEGER NOT NULL," +
                "satellites_visible INTEGER," +
                "satellites_used INTEGER," +
                "avg_cn0_dbhz REAL," +
                "max_cn0_dbhz REAL," +
                "gps_count INTEGER," +
                "galileo_count INTEGER," +
                "glonass_count INTEGER," +
                "beidou_count INTEGER," +
                "qzss_count INTEGER," +
                "synced INTEGER NOT NULL DEFAULT 0," +
                "UNIQUE(session_id, seq)" +
                ")");

        db.execSQL("CREATE TABLE sensor_sample (" +
                "id INTEGER PRIMARY KEY AUTOINCREMENT," +
                "session_id TEXT NOT NULL," +
                "seq INTEGER NOT NULL," +
                "utc_ms INTEGER NOT NULL," +
                "elapsed_realtime_ns INTEGER NOT NULL," +
                "sensor_type TEXT NOT NULL," +
                "x REAL," +
                "y REAL," +
                "z REAL," +
                "w REAL," +
                "accuracy INTEGER," +
                "synced INTEGER NOT NULL DEFAULT 0," +
                "UNIQUE(session_id, seq)" +
                ")");

        db.execSQL("CREATE INDEX idx_location_unsynced ON location_sample(session_id, synced, seq)");
        db.execSQL("CREATE INDEX idx_gnss_unsynced ON gnss_snapshot(session_id, synced, seq)");
        db.execSQL("CREATE INDEX idx_sensor_unsynced ON sensor_sample(session_id, synced, seq)");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        throw new IllegalStateException("No migration defined from " + oldVersion + " to " + newVersion);
    }

    void startSession(String sessionId, String installId, long utcMs, long elapsedNs,
                      String appVersion, String sensorCapabilities) {
        ContentValues v = new ContentValues();
        v.put("session_id", sessionId);
        v.put("device_install_id", installId);
        v.put("started_at_utc_ms", utcMs);
        v.put("started_elapsed_ns", elapsedNs);
        v.put("app_version", appVersion);
        v.put("device_model", Build.MANUFACTURER + " " + Build.MODEL);
        v.put("android_version", Build.VERSION.RELEASE + " (SDK " + Build.VERSION.SDK_INT + ")");
        v.put("sensor_capabilities", sensorCapabilities);
        v.put("state", "RECORDING");
        getWritableDatabase().insertOrThrow("session", null, v);
    }

    void endSession(String sessionId, long utcMs, long elapsedNs, String state) {
        ContentValues v = new ContentValues();
        v.put("ended_at_utc_ms", utcMs);
        v.put("ended_elapsed_ns", elapsedNs);
        v.put("state", state);
        getWritableDatabase().update("session", v, "session_id=?", new String[]{sessionId});
    }

    void insertLocation(String sessionId, long seq, Location l) {
        ContentValues v = new ContentValues();
        v.put("session_id", sessionId);
        v.put("seq", seq);
        v.put("fix_utc_ms", l.getTime());
        v.put("elapsed_realtime_ns", Build.VERSION.SDK_INT >= 17 ? l.getElapsedRealtimeNanos() : 0L);
        v.put("provider", l.getProvider());
        v.put("latitude", l.getLatitude());
        v.put("longitude", l.getLongitude());
        if (l.hasAccuracy()) v.put("horizontal_accuracy_m", l.getAccuracy());
        if (l.hasAltitude()) v.put("altitude_wgs84_m", l.getAltitude());
        if (Build.VERSION.SDK_INT >= 26 && l.hasVerticalAccuracy()) v.put("vertical_accuracy_m", l.getVerticalAccuracyMeters());
        if (l.hasSpeed()) v.put("speed_mps", l.getSpeed());
        if (Build.VERSION.SDK_INT >= 26 && l.hasSpeedAccuracy()) v.put("speed_accuracy_mps", l.getSpeedAccuracyMetersPerSecond());
        if (l.hasBearing()) v.put("bearing_deg", l.getBearing());
        if (Build.VERSION.SDK_INT >= 26 && l.hasBearingAccuracy()) v.put("bearing_accuracy_deg", l.getBearingAccuracyDegrees());
        if (Build.VERSION.SDK_INT >= 18) v.put("is_mock", l.isFromMockProvider() ? 1 : 0);
        getWritableDatabase().insertOrThrow("location_sample", null, v);
    }

    void insertGnss(String sessionId, long seq, long utcMs, long elapsedNs,
                    int visible, int used, double avgCn0, double maxCn0,
                    int gps, int galileo, int glonass, int beidou, int qzss) {
        ContentValues v = new ContentValues();
        v.put("session_id", sessionId);
        v.put("seq", seq);
        v.put("utc_ms", utcMs);
        v.put("elapsed_realtime_ns", elapsedNs);
        v.put("satellites_visible", visible);
        v.put("satellites_used", used);
        v.put("avg_cn0_dbhz", avgCn0);
        v.put("max_cn0_dbhz", maxCn0);
        v.put("gps_count", gps);
        v.put("galileo_count", galileo);
        v.put("glonass_count", glonass);
        v.put("beidou_count", beidou);
        v.put("qzss_count", qzss);
        getWritableDatabase().insertOrThrow("gnss_snapshot", null, v);
    }

    void insertSensor(String sessionId, long seq, long utcMs, long elapsedNs,
                      String type, float[] values, int accuracy) {
        ContentValues v = new ContentValues();
        v.put("session_id", sessionId);
        v.put("seq", seq);
        v.put("utc_ms", utcMs);
        v.put("elapsed_realtime_ns", elapsedNs);
        v.put("sensor_type", type);
        if (values.length > 0) v.put("x", values[0]);
        if (values.length > 1) v.put("y", values[1]);
        if (values.length > 2) v.put("z", values[2]);
        if (values.length > 3) v.put("w", values[3]);
        v.put("accuracy", accuracy);
        getWritableDatabase().insertOrThrow("sensor_sample", null, v);
    }

    long count(String table, String sessionId) {
        try (Cursor c = getReadableDatabase().rawQuery(
                "SELECT COUNT(*) FROM " + table + " WHERE session_id=?",
                new String[]{sessionId})) {
            return c.moveToFirst() ? c.getLong(0) : 0L;
        }
    }

    SessionRecord pendingSessionStart(String sessionId) {
        try (Cursor c = getReadableDatabase().rawQuery(
                "SELECT session_id,device_install_id,started_at_utc_ms,started_elapsed_ns,app_version,device_model,android_version,sensor_capabilities " +
                        "FROM session WHERE session_id=? AND start_synced=0 LIMIT 1",
                new String[]{sessionId})) {
            if (!c.moveToFirst()) return null;
            JSONObject p = new JSONObject();
            try {
                p.put("session_id", c.getString(0));
                p.put("device_install_id", c.getString(1));
                p.put("started_at_utc_ms", c.getLong(2));
                p.put("started_elapsed_ns", c.getLong(3));
                p.put("app_version", c.getString(4));
                p.put("device_model", c.getString(5));
                p.put("android_version", c.getString(6));
                p.put("sensor_capabilities", c.getString(7));
            } catch (Exception ignored) {}
            return new SessionRecord(c.getString(0), p);
        }
    }

    SessionRecord pendingSessionEnd(String sessionId) {
        try (Cursor c = getReadableDatabase().rawQuery(
                "SELECT session_id,ended_at_utc_ms,ended_elapsed_ns,state FROM session " +
                        "WHERE session_id=? AND ended_at_utc_ms IS NOT NULL AND end_synced=0 LIMIT 1",
                new String[]{sessionId})) {
            if (!c.moveToFirst()) return null;
            JSONObject p = new JSONObject();
            try {
                p.put("session_id", c.getString(0));
                p.put("ended_at_utc_ms", c.getLong(1));
                p.put("ended_elapsed_ns", c.getLong(2));
                p.put("state", c.getString(3));
            } catch (Exception ignored) {}
            return new SessionRecord(c.getString(0), p);
        }
    }

    void markSessionStartSynced(String sessionId) {
        ContentValues v = new ContentValues();
        v.put("start_synced", 1);
        getWritableDatabase().update("session", v, "session_id=?", new String[]{sessionId});
    }

    void markSessionEndSynced(String sessionId) {
        ContentValues v = new ContentValues();
        v.put("end_synced", 1);
        getWritableDatabase().update("session", v, "session_id=?", new String[]{sessionId});
    }

    Batch pendingBatch(String table, String streamType, String sessionId, int limit) {
        String order = "seq ASC LIMIT " + Math.max(1, limit);
        try (Cursor c = getReadableDatabase().query(table, null,
                "session_id=? AND synced=0", new String[]{sessionId}, null, null, order)) {
            if (!c.moveToFirst()) return null;
            JSONArray arr = new JSONArray();
            long first = -1L;
            long last = -1L;
            do {
                JSONObject o = new JSONObject();
                long seq = c.getLong(c.getColumnIndexOrThrow("seq"));
                if (first < 0) first = seq;
                last = seq;
                try {
                    o.put("seq", seq);
                    for (int i = 0; i < c.getColumnCount(); i++) {
                        String col = c.getColumnName(i);
                        if ("id".equals(col) || "session_id".equals(col) || "seq".equals(col) || "synced".equals(col)) continue;
                        if (c.isNull(i)) {
                            o.put(col, JSONObject.NULL);
                        } else {
                            switch (c.getType(i)) {
                                case Cursor.FIELD_TYPE_INTEGER: o.put(col, c.getLong(i)); break;
                                case Cursor.FIELD_TYPE_FLOAT: o.put(col, c.getDouble(i)); break;
                                case Cursor.FIELD_TYPE_STRING: o.put(col, c.getString(i)); break;
                                default: o.put(col, c.getString(i));
                            }
                        }
                    }
                } catch (Exception ignored) {}
                arr.put(o);
            } while (c.moveToNext());
            return new Batch(table, streamType, sessionId, first, last, arr);
        }
    }

    void markBatchSynced(String table, String sessionId, long throughSeq) {
        ContentValues v = new ContentValues();
        v.put("synced", 1);
        getWritableDatabase().update(table, v,
                "session_id=? AND seq<=?",
                new String[]{sessionId, Long.toString(throughSeq)});
    }

    long pendingCount(String sessionId) {
        long total = 0;
        total += countPending("location_sample", sessionId);
        total += countPending("gnss_snapshot", sessionId);
        total += countPending("sensor_sample", sessionId);
        return total;
    }

    private long countPending(String table, String sessionId) {
        try (Cursor c = getReadableDatabase().rawQuery(
                "SELECT COUNT(*) FROM " + table + " WHERE session_id=? AND synced=0",
                new String[]{sessionId})) {
            return c.moveToFirst() ? c.getLong(0) : 0L;
        }
    }

    String latestSessionId() {
        try (Cursor c = getReadableDatabase().rawQuery(
                "SELECT session_id FROM session ORDER BY started_at_utc_ms DESC LIMIT 1", null)) {
            return c.moveToFirst() ? c.getString(0) : null;
        }
    }

    static final class Batch {
        final String table;
        final String streamType;
        final String sessionId;
        final long firstSeq;
        final long lastSeq;
        final JSONArray samples;

        Batch(String table, String streamType, String sessionId, long firstSeq, long lastSeq, JSONArray samples) {
            this.table = table;
            this.streamType = streamType;
            this.sessionId = sessionId;
            this.firstSeq = firstSeq;
            this.lastSeq = lastSeq;
            this.samples = samples;
        }
    }

    static final class SessionRecord {
        final String sessionId;
        final JSONObject payload;

        SessionRecord(String sessionId, JSONObject payload) {
            this.sessionId = sessionId;
            this.payload = payload;
        }
    }
}
