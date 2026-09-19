package com.bryce.signallogger;

import android.Manifest;
import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.hardware.Sensor;
import android.hardware.SensorEvent;
import android.hardware.SensorEventListener;
import android.hardware.SensorManager;
import android.location.GnssStatus;
import android.location.Location;
import android.location.LocationListener;
import android.location.LocationManager;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.HandlerThread;
import android.os.IBinder;
import android.os.PowerManager;
import android.os.SystemClock;

import org.json.JSONArray;
import org.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoggerService extends Service implements LocationListener, SensorEventListener {
    public static final String ACTION_START = "com.bryce.signallogger.START";
    public static final String ACTION_STOP = "com.bryce.signallogger.STOP";
    public static final String ACTION_STATUS = "com.bryce.signallogger.STATUS";
    public static final String PREFS = "signal_logger_prefs";
    public static final String PREF_ENDPOINT = "mcp_endpoint";
    public static final String PREF_TOKEN = "mcp_token";
    public static final String PREF_BACKGROUND_ACTIVE = "background_recording_active";
    public static final String PREF_ACTIVE_SESSION = "active_session_id";
    public static final String PREF_ACTIVE_STARTED_MS = "active_started_at_ms";

    private static final String CHANNEL_ID = "signal_logger_recording";
    private static final int NOTIFICATION_ID = 4101;

    private HandlerThread workerThread;
    private Handler worker;
    private LocationManager locationManager;
    private SensorManager sensorManager;
    private SignalDatabase db;
    private ScheduledExecutorService syncExecutor;
    private McpUploader uploader;
    private PowerManager.WakeLock wakeLock;

    private final AtomicBoolean recording = new AtomicBoolean(false);
    private String sessionId;
    private long locationSeq = 0;
    private long gnssSeq = 0;
    private long sensorSeq = 0;
    private long startedAtMs = 0;

    private volatile float latestAccuracy = Float.NaN;
    private volatile float latestSpeedMps = Float.NaN;
    private volatile float latestBearing = Float.NaN;
    private volatile int latestSatVisible = 0;
    private volatile int latestSatUsed = 0;
    private volatile String syncStatus = "NOT CONFIGURED";
    private volatile long syncPending = 0;
    private volatile String syncAck = "GPS 0 · GNSS 0 · Sensor 0";
    private volatile long syncLastSuccessMs = 0L;
    private volatile String lastError = "";

    private GnssStatus.Callback gnssCallback;

    @Override
    public void onCreate() {
        super.onCreate();
        createNotificationChannel();
        db = new SignalDatabase(this);
        locationManager = (LocationManager) getSystemService(Context.LOCATION_SERVICE);
        sensorManager = (SensorManager) getSystemService(Context.SENSOR_SERVICE);
        workerThread = new HandlerThread("SignalLoggerWorker");
        workerThread.start();
        worker = new Handler(workerThread.getLooper());
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
        String action = intent == null ? null : intent.getAction();
        if (ACTION_STOP.equals(action)) {
            stopRecording("COMPLETED");
            stopSelf();
            return START_NOT_STICKY;
        }

        boolean restartAfterSystemKill = intent == null && prefs.getBoolean(PREF_BACKGROUND_ACTIVE, false);
        if ((ACTION_START.equals(action) || restartAfterSystemKill) && recording.compareAndSet(false, true)) {
            startForeground(NOTIFICATION_ID, buildNotification(restartAfterSystemKill ? "Restoring background session…" : "Starting…"));
            worker.post(() -> beginRecording(restartAfterSystemKill));
        }
        return recording.get() || prefs.getBoolean(PREF_BACKGROUND_ACTIVE, false) ? START_STICKY : START_NOT_STICKY;
    }

    private void beginRecording(boolean resumeExistingSession) {
        try {
            if (Build.VERSION.SDK_INT >= 23 && checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
                throw new SecurityException("Fine location permission not granted");
            }

            SharedPreferences prefs = getSharedPreferences(PREFS, MODE_PRIVATE);
            String installId = prefs.getString("install_id", null);
            if (installId == null) {
                installId = UUID.randomUUID().toString();
                prefs.edit().putString("install_id", installId).apply();
            }

            if (resumeExistingSession) {
                sessionId = prefs.getString(PREF_ACTIVE_SESSION, "");
                if (sessionId == null || sessionId.isEmpty()) resumeExistingSession = false;
            }

            if (resumeExistingSession) {
                startedAtMs = prefs.getLong(PREF_ACTIVE_STARTED_MS, db.sessionStartedAt(sessionId));
                locationSeq = db.maxSeq("location_sample", sessionId);
                gnssSeq = db.maxSeq("gnss_snapshot", sessionId);
                sensorSeq = db.maxSeq("sensor_sample", sessionId);
            } else {
                sessionId = newSessionId();
                startedAtMs = System.currentTimeMillis();
                db.startSession(sessionId, installId, startedAtMs, SystemClock.elapsedRealtimeNanos(),
                        "0.1.3", sensorCapabilities());
                prefs.edit()
                        .putBoolean(PREF_BACKGROUND_ACTIVE, true)
                        .putString(PREF_ACTIVE_SESSION, sessionId)
                        .putLong(PREF_ACTIVE_STARTED_MS, startedAtMs)
                        .apply();
            }

            acquireWakeLock();
            // Research data must stay GNSS-only. Mixing NETWORK_PROVIDER fixes into
            // the same stream creates false jumps between parallel roads / elevated roads.
            // 500 ms sampling also gives sub-second resolution for stop/release timing.
            locationManager.requestLocationUpdates(
                    LocationManager.GPS_PROVIDER, 500L, 0f, this, workerThread.getLooper());

            registerGnss();
            registerSensors();

            uploader = new McpUploader(db,
                    prefs.getString(PREF_ENDPOINT, ""),
                    prefs.getString(PREF_TOKEN, ""));
            syncExecutor = Executors.newSingleThreadScheduledExecutor();
            syncExecutor.scheduleWithFixedDelay(this::runSync, 0, 2, TimeUnit.SECONDS);
            worker.post(statusTicker);
            updateNotification();
        } catch (Exception e) {
            lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
            recording.set(false);
            releaseWakeLock();
            getSharedPreferences(PREFS, MODE_PRIVATE).edit().putBoolean(PREF_BACKGROUND_ACTIVE, false).apply();
            broadcastStatus();
            stopSelf();
        }
    }

    private void registerGnss() {
        if (Build.VERSION.SDK_INT < 24) return;
        gnssCallback = new GnssStatus.Callback() {
            @Override
            public void onSatelliteStatusChanged(GnssStatus status) {
                if (!recording.get() || sessionId == null) return;
                int visible = status.getSatelliteCount();
                int used = 0, gps = 0, galileo = 0, glonass = 0, beidou = 0, qzss = 0;
                double sum = 0.0, max = 0.0;
                for (int i = 0; i < visible; i++) {
                    if (status.usedInFix(i)) used++;
                    float cn0 = status.getCn0DbHz(i);
                    sum += cn0;
                    if (cn0 > max) max = cn0;
                    switch (status.getConstellationType(i)) {
                        case GnssStatus.CONSTELLATION_GPS: gps++; break;
                        case GnssStatus.CONSTELLATION_GALILEO: galileo++; break;
                        case GnssStatus.CONSTELLATION_GLONASS: glonass++; break;
                        case GnssStatus.CONSTELLATION_BEIDOU: beidou++; break;
                        case GnssStatus.CONSTELLATION_QZSS: qzss++; break;
                        default: break;
                    }
                }
                latestSatVisible = visible;
                latestSatUsed = used;
                gnssSeq++;
                db.insertGnss(sessionId, gnssSeq, System.currentTimeMillis(), SystemClock.elapsedRealtimeNanos(),
                        visible, used, visible == 0 ? 0.0 : sum / visible, max,
                        gps, galileo, glonass, beidou, qzss);
            }
        };
        try {
            locationManager.registerGnssStatusCallback(gnssCallback, worker);
        } catch (SecurityException e) {
            lastError = "GNSS status unavailable: " + e.getMessage();
        }
    }

    private void registerSensors() {
        registerSensor(Sensor.TYPE_ACCELEROMETER, 100_000);
        registerSensor(Sensor.TYPE_GYROSCOPE, 100_000);
        registerSensor(Sensor.TYPE_ROTATION_VECTOR, 200_000);
        registerSensor(Sensor.TYPE_MAGNETIC_FIELD, 200_000);
        registerSensor(Sensor.TYPE_PRESSURE, 1_000_000);
    }

    private void registerSensor(int type, int periodUs) {
        Sensor sensor = sensorManager.getDefaultSensor(type);
        if (sensor != null) {
            sensorManager.registerListener(this, sensor, periodUs, 5_000_000, worker);
        }
    }

    @Override
    public void onLocationChanged(Location location) {
        if (!recording.get() || sessionId == null || location == null) return;
        if (!LocationManager.GPS_PROVIDER.equals(location.getProvider())) return;
        try {
            locationSeq++;
            db.insertLocation(sessionId, locationSeq, location);
            latestAccuracy = location.hasAccuracy() ? location.getAccuracy() : Float.NaN;
            latestSpeedMps = location.hasSpeed() ? location.getSpeed() : Float.NaN;
            latestBearing = location.hasBearing() ? location.getBearing() : Float.NaN;
        } catch (Exception e) {
            lastError = "Location DB: " + e.getMessage();
        }
    }

    @Override public void onStatusChanged(String provider, int status, Bundle extras) {}
    @Override public void onProviderEnabled(String provider) {}
    @Override public void onProviderDisabled(String provider) {}

    @Override
    public void onSensorChanged(SensorEvent event) {
        if (!recording.get() || sessionId == null) return;
        try {
            sensorSeq++;
            long utcMs = System.currentTimeMillis() + (event.timestamp - SystemClock.elapsedRealtimeNanos()) / 1_000_000L;
            float[] values = event.values.clone();
            db.insertSensor(sessionId, sensorSeq, utcMs, event.timestamp,
                    sensorName(event.sensor.getType()), values, event.accuracy);
        } catch (Exception e) {
            lastError = "Sensor DB: " + e.getMessage();
        }
    }

    @Override
    public void onAccuracyChanged(Sensor sensor, int accuracy) {}

    private void runSync() {
        try {
            if (!recording.get() || uploader == null || sessionId == null) return;
            McpUploader.SyncResult r = uploader.syncOnce(sessionId);
            syncStatus = r.status;
            syncPending = r.pending;
            syncAck = r.ackSummary;
            syncLastSuccessMs = r.lastSuccessAtMs;
            if (r.online) {
                if (lastError != null && lastError.startsWith("Sync: ")) lastError = "";
            } else {
                lastError = "Sync: " + r.error;
            }
        } catch (Exception e) {
            syncStatus = "SYNC ERROR";
            lastError = "Sync: " + e.getMessage();
        }
    }

    private final Runnable statusTicker = new Runnable() {
        @Override public void run() {
            if (!recording.get()) return;
            broadcastStatus();
            updateNotification();
            worker.postDelayed(this, 1000L);
        }
    };

    private void broadcastStatus() {
        Intent i = new Intent(ACTION_STATUS);
        i.setPackage(getPackageName());
        i.putExtra("recording", recording.get());
        i.putExtra("session_id", sessionId == null ? "" : sessionId);
        i.putExtra("duration_ms", startedAtMs == 0 ? 0 : System.currentTimeMillis() - startedAtMs);
        i.putExtra("location_samples", sessionId == null ? 0 : db.count("location_sample", sessionId));
        i.putExtra("sensor_samples", sessionId == null ? 0 : db.count("sensor_sample", sessionId));
        i.putExtra("gnss_samples", sessionId == null ? 0 : db.count("gnss_snapshot", sessionId));
        i.putExtra("accuracy", latestAccuracy);
        i.putExtra("speed_mps", latestSpeedMps);
        i.putExtra("bearing", latestBearing);
        i.putExtra("sat_visible", latestSatVisible);
        i.putExtra("sat_used", latestSatUsed);
        i.putExtra("sync_status", syncStatus);
        i.putExtra("sync_pending", syncPending);
        i.putExtra("sync_ack", syncAck);
        i.putExtra("sync_last_success_ms", syncLastSuccessMs);
        i.putExtra("last_error", lastError);
        sendBroadcast(i);
    }

    private void stopRecording(String state) {
        if (!recording.compareAndSet(true, false)) return;
        try { locationManager.removeUpdates(this); } catch (Exception ignored) {}
        try {
            if (Build.VERSION.SDK_INT >= 24 && gnssCallback != null) locationManager.unregisterGnssStatusCallback(gnssCallback);
        } catch (Exception ignored) {}
        try { sensorManager.unregisterListener(this); } catch (Exception ignored) {}
        worker.removeCallbacks(statusTicker);

        if (sessionId != null) {
            db.endSession(sessionId, System.currentTimeMillis(), SystemClock.elapsedRealtimeNanos(), state);
            if (uploader != null && uploader.isConfigured()) {
                // One final best-effort pass; unsent rows remain local if the network is down.
                try { uploader.syncOnce(sessionId); } catch (Exception ignored) {}
            }
        }
        if (syncExecutor != null) {
            syncExecutor.shutdown();
            syncExecutor = null;
        }
        getSharedPreferences(PREFS, MODE_PRIVATE).edit()
                .putBoolean(PREF_BACKGROUND_ACTIVE, false)
                .remove(PREF_ACTIVE_SESSION)
                .remove(PREF_ACTIVE_STARTED_MS)
                .apply();
        releaseWakeLock();
        broadcastStatus();
        stopForeground(true);
    }

    private void acquireWakeLock() {
        if (wakeLock != null && wakeLock.isHeld()) return;
        PowerManager pm = (PowerManager) getSystemService(Context.POWER_SERVICE);
        wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "SignalLogger::BackgroundRecording");
        wakeLock.setReferenceCounted(false);
        wakeLock.acquire();
    }

    private void releaseWakeLock() {
        try {
            if (wakeLock != null && wakeLock.isHeld()) wakeLock.release();
        } catch (Exception ignored) {}
        wakeLock = null;
    }

    @Override
    public void onDestroy() {
        if (recording.get()) {
            // Preserve the active-session preference so START_STICKY can restore the same session
            // after a system/vendor background kill. A user STOP clears the preference above.
            try { locationManager.removeUpdates(this); } catch (Exception ignored) {}
            try {
                if (Build.VERSION.SDK_INT >= 24 && gnssCallback != null) locationManager.unregisterGnssStatusCallback(gnssCallback);
            } catch (Exception ignored) {}
            try { sensorManager.unregisterListener(this); } catch (Exception ignored) {}
            if (syncExecutor != null) syncExecutor.shutdownNow();
            releaseWakeLock();
            recording.set(false);
        }
        if (workerThread != null) workerThread.quitSafely();
        if (db != null) db.close();
        super.onDestroy();
    }

    @Override
    public IBinder onBind(Intent intent) {
        return null;
    }

    private String sensorCapabilities() {
        JSONArray a = new JSONArray();
        int[] types = new int[]{Sensor.TYPE_ACCELEROMETER, Sensor.TYPE_GYROSCOPE,
                Sensor.TYPE_ROTATION_VECTOR, Sensor.TYPE_MAGNETIC_FIELD, Sensor.TYPE_PRESSURE};
        for (int type : types) {
            Sensor s = sensorManager.getDefaultSensor(type);
            JSONObject o = new JSONObject();
            try {
                o.put("type", sensorName(type));
                o.put("available", s != null);
                if (s != null) {
                    o.put("name", s.getName());
                    o.put("vendor", s.getVendor());
                    o.put("version", s.getVersion());
                    o.put("min_delay_us", s.getMinDelay());
                }
                a.put(o);
            } catch (Exception ignored) {}
        }
        return a.toString();
    }

    static String sensorName(int type) {
        switch (type) {
            case Sensor.TYPE_ACCELEROMETER: return "ACCELEROMETER";
            case Sensor.TYPE_GYROSCOPE: return "GYROSCOPE";
            case Sensor.TYPE_ROTATION_VECTOR: return "ROTATION_VECTOR";
            case Sensor.TYPE_MAGNETIC_FIELD: return "MAGNETOMETER";
            case Sensor.TYPE_PRESSURE: return "PRESSURE";
            default: return "SENSOR_" + type;
        }
    }

    private static String newSessionId() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss", Locale.US);
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Taipei"));
        return "S" + sdf.format(new Date()) + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private void createNotificationChannel() {
        if (Build.VERSION.SDK_INT >= 26) {
            NotificationChannel ch = new NotificationChannel(CHANNEL_ID, "SignalLogger recording",
                    NotificationManager.IMPORTANCE_LOW);
            ch.setDescription("Keeps GPS logging active while the screen is off");
            getSystemService(NotificationManager.class).createNotificationChannel(ch);
        }
    }

    private Notification buildNotification(String text) {
        Intent open = new Intent(this, MainActivity.class);
        PendingIntent pi = PendingIntent.getActivity(this, 0, open,
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        Notification.Builder b = Build.VERSION.SDK_INT >= 26
                ? new Notification.Builder(this, CHANNEL_ID)
                : new Notification.Builder(this);
        return b.setContentTitle("SignalLogger recording")
                .setContentText(text)
                .setSmallIcon(android.R.drawable.ic_menu_mylocation)
                .setOngoing(true)
                .setContentIntent(pi)
                .build();
    }

    private void updateNotification() {
        if (!recording.get()) return;
        long samples = sessionId == null ? 0 : db.count("location_sample", sessionId);
        String text = samples + " GPS samples · " + syncStatus + " · pending " + syncPending;
        NotificationManager nm = (NotificationManager) getSystemService(NOTIFICATION_SERVICE);
        nm.notify(NOTIFICATION_ID, buildNotification(text));
    }
}
