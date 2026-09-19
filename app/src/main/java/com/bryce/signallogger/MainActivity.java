package com.bryce.signallogger;

import android.Manifest;
import android.app.Activity;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.SharedPreferences;
import android.content.pm.PackageManager;
import android.graphics.Color;
import android.hardware.Sensor;
import android.hardware.SensorManager;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.Environment;
import android.os.PowerManager;
import android.provider.Settings;
import android.view.Gravity;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class MainActivity extends Activity {
    private static final int REQ_LOCATION = 2001;

    private TextView recordingView;
    private TextView sessionView;
    private TextView gpsView;
    private TextView gnssView;
    private TextView samplesView;
    private TextView syncView;
    private TextView sensorView;
    private TextView backgroundView;
    private TextView errorView;
    private EditText endpointEdit;
    private EditText tokenEdit;
    private Button startButton;
    private Button stopButton;
    private boolean pendingStart = false;

    private final BroadcastReceiver statusReceiver = new BroadcastReceiver() {
        @Override public void onReceive(Context context, Intent intent) {
            if (!LoggerService.ACTION_STATUS.equals(intent.getAction())) return;
            updateFromStatus(intent);
        }
    };

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(buildUi());
        loadSettings();
        refreshHardwareProbe();
        refreshBackgroundStatus();
    }

    @Override
    protected void onResume() {
        super.onResume();
        refreshBackgroundStatus();
    }

    @Override
    protected void onStart() {
        super.onStart();
        IntentFilter f = new IntentFilter(LoggerService.ACTION_STATUS);
        if (Build.VERSION.SDK_INT >= 33) {
            registerReceiver(statusReceiver, f, Context.RECEIVER_NOT_EXPORTED);
        } else {
            registerReceiver(statusReceiver, f);
        }
    }

    @Override
    protected void onStop() {
        try { unregisterReceiver(statusReceiver); } catch (Exception ignored) {}
        saveSettings();
        super.onStop();
    }

    private View buildUi() {
        ScrollView scroll = new ScrollView(this);
        LinearLayout root = new LinearLayout(this);
        root.setOrientation(LinearLayout.VERTICAL);
        root.setPadding(dp(20), dp(18), dp(20), dp(30));
        scroll.addView(root);

        TextView title = text("SIGNAL LOGGER", 26, true);
        root.addView(title);
        TextView subtitle = text("TaipeiSignal · research logger · local-first", 13, false);
        subtitle.setTextColor(Color.DKGRAY);
        root.addView(subtitle);

        recordingView = text("● IDLE", 21, true);
        recordingView.setPadding(0, dp(20), 0, dp(8));
        root.addView(recordingView);

        sessionView = text("Session: --", 13, false);
        root.addView(sessionView);

        addSection(root, "GPS / GNSS");
        gpsView = text("Accuracy: --\nSpeed: --\nBearing: --", 17, false);
        root.addView(gpsView);
        gnssView = text("Satellites: -- / --", 15, false);
        root.addView(gnssView);

        addSection(root, "ROAD MATCH");
        TextView road = text("Road: --\nDirection: --\nStructure: UNKNOWN\n(Matcher will be added after raw-log validation)", 15, false);
        root.addView(road);

        addSection(root, "SESSION");
        samplesView = text("GPS: 0\nGNSS: 0\nSensor: 0", 15, false);
        root.addView(samplesView);

        addSection(root, "REALTIME → MAC MCP");
        endpointEdit = new EditText(this);
        endpointEdit.setHint("MCP URL, e.g. https://…/mcp");
        endpointEdit.setSingleLine(true);
        root.addView(endpointEdit, fullWidth());
        tokenEdit = new EditText(this);
        tokenEdit.setHint("Bearer token (optional on LAN)");
        tokenEdit.setSingleLine(true);
        tokenEdit.setInputType(android.text.InputType.TYPE_CLASS_TEXT | android.text.InputType.TYPE_TEXT_VARIATION_PASSWORD);
        root.addView(tokenEdit, fullWidth());
        syncView = text("Sync: NOT CONFIGURED\nPending: 0\nACK: --\nLast success: --", 15, false);
        root.addView(syncView);

        addSection(root, "BACKGROUND EXECUTION");
        backgroundView = text("Checking…", 14, false);
        root.addView(backgroundView);
        Button background = button("ENABLE BACKGROUND MODE");
        background.setOnClickListener(v -> requestBackgroundMode());
        root.addView(background, fullWidth());

        addSection(root, "HARDWARE PROBE");
        sensorView = text("Checking…", 14, false);
        root.addView(sensorView);

        errorView = text("", 13, false);
        errorView.setTextColor(Color.rgb(170, 30, 30));
        errorView.setPadding(0, dp(12), 0, 0);
        root.addView(errorView);

        LinearLayout row = new LinearLayout(this);
        row.setOrientation(LinearLayout.HORIZONTAL);
        row.setGravity(Gravity.CENTER);
        row.setPadding(0, dp(18), 0, 0);
        startButton = button("START");
        stopButton = button("STOP");
        stopButton.setEnabled(false);
        row.addView(startButton, weighted());
        row.addView(stopButton, weighted());
        root.addView(row);

        Button export = button("EXPORT LATEST SESSION DB");
        export.setOnClickListener(v -> exportDatabase());
        root.addView(export, fullWidth());

        startButton.setOnClickListener(v -> startLogger());
        stopButton.setOnClickListener(v -> stopLogger());
        return scroll;
    }

    private void startLogger() {
        saveSettings();
        if (Build.VERSION.SDK_INT >= 23 && checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) != PackageManager.PERMISSION_GRANTED) {
            pendingStart = true;
            if (Build.VERSION.SDK_INT >= 33) {
                requestPermissions(new String[]{Manifest.permission.ACCESS_FINE_LOCATION, Manifest.permission.POST_NOTIFICATIONS}, REQ_LOCATION);
            } else {
                requestPermissions(new String[]{Manifest.permission.ACCESS_FINE_LOCATION}, REQ_LOCATION);
            }
            return;
        }
        Intent i = new Intent(this, LoggerService.class).setAction(LoggerService.ACTION_START);
        if (Build.VERSION.SDK_INT >= 26) startForegroundService(i); else startService(i);
        recordingView.setText("● STARTING");
        startButton.setEnabled(false);
        stopButton.setEnabled(true);
    }

    private void stopLogger() {
        Intent i = new Intent(this, LoggerService.class).setAction(LoggerService.ACTION_STOP);
        startService(i);
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        if (requestCode == REQ_LOCATION) {
            boolean granted = checkSelfPermission(Manifest.permission.ACCESS_FINE_LOCATION) == PackageManager.PERMISSION_GRANTED;
            if (granted && pendingStart) {
                pendingStart = false;
                startLogger();
            } else {
                pendingStart = false;
                Toast.makeText(this, "Precise location is required for SignalLogger.", Toast.LENGTH_LONG).show();
            }
        }
    }

    private void updateFromStatus(Intent i) {
        boolean recording = i.getBooleanExtra("recording", false);
        long duration = i.getLongExtra("duration_ms", 0L);
        recordingView.setText(recording ? "● RECORDING  " + formatDuration(duration) : "● IDLE");
        recordingView.setTextColor(recording ? Color.rgb(185, 20, 20) : Color.BLACK);
        sessionView.setText("Session: " + valueOrDash(i.getStringExtra("session_id")));

        float acc = i.getFloatExtra("accuracy", Float.NaN);
        float speed = i.getFloatExtra("speed_mps", Float.NaN);
        float bearing = i.getFloatExtra("bearing", Float.NaN);
        gpsView.setText("Accuracy: " + fmt(acc, "m") +
                "\nSpeed: " + (Float.isNaN(speed) ? "--" : String.format(Locale.US, "%.1f km/h", speed * 3.6f)) +
                "\nBearing: " + fmt(bearing, "°"));
        gnssView.setText("Satellites: " + i.getIntExtra("sat_used", 0) + " used / " + i.getIntExtra("sat_visible", 0) + " visible");
        samplesView.setText("GPS: " + i.getLongExtra("location_samples", 0) +
                "\nGNSS: " + i.getLongExtra("gnss_samples", 0) +
                "\nSensor: " + i.getLongExtra("sensor_samples", 0));
        long lastSyncMs = i.getLongExtra("sync_last_success_ms", 0L);
        String lastSync = lastSyncMs <= 0L ? "--" :
                Math.max(0L, (System.currentTimeMillis() - lastSyncMs) / 1000L) + "s ago";
        syncView.setText("Sync: " + i.getStringExtra("sync_status") +
                "\nPending: " + i.getLongExtra("sync_pending", 0) +
                "\nACK: " + valueOrDash(i.getStringExtra("sync_ack")) +
                "\nLast success: " + lastSync);
        String err = i.getStringExtra("last_error");
        errorView.setText(err == null || err.isEmpty() ? "" : "Last error: " + err);
        startButton.setEnabled(!recording);
        stopButton.setEnabled(recording);
    }

    private void refreshBackgroundStatus() {
        if (backgroundView == null) return;
        PowerManager pm = (PowerManager) getSystemService(POWER_SERVICE);
        boolean unrestricted = Build.VERSION.SDK_INT < 23 || pm.isIgnoringBatteryOptimizations(getPackageName());
        SharedPreferences p = getSharedPreferences(LoggerService.PREFS, MODE_PRIVATE);
        boolean active = p.getBoolean(LoggerService.PREF_BACKGROUND_ACTIVE, false);
        backgroundView.setText("Foreground service: ENABLED\n" +
                "Screen-off CPU lock while recording: ENABLED\n" +
                "Battery optimization exemption: " + (unrestricted ? "ENABLED" : "NOT ENABLED") + "\n" +
                "Background session requested: " + (active ? "YES" : "NO"));
    }

    private void requestBackgroundMode() {
        try {
            if (Build.VERSION.SDK_INT >= 23) {
                PowerManager pm = (PowerManager) getSystemService(POWER_SERVICE);
                if (!pm.isIgnoringBatteryOptimizations(getPackageName())) {
                    Intent request = new Intent(Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS,
                            Uri.parse("package:" + getPackageName()));
                    startActivity(request);
                    return;
                }
            }
            startActivity(new Intent(Settings.ACTION_IGNORE_BATTERY_OPTIMIZATION_SETTINGS));
        } catch (Exception e) {
            try {
                Intent details = new Intent(Settings.ACTION_APPLICATION_DETAILS_SETTINGS,
                        Uri.parse("package:" + getPackageName()));
                startActivity(details);
            } catch (Exception ignored) {
                Toast.makeText(this, "Open system battery settings and allow SignalLogger to run in background.", Toast.LENGTH_LONG).show();
            }
        }
    }

    private void refreshHardwareProbe() {
        SensorManager sm = (SensorManager) getSystemService(SENSOR_SERVICE);
        StringBuilder sb = new StringBuilder();
        appendSensor(sb, sm, Sensor.TYPE_ACCELEROMETER, "Accelerometer");
        appendSensor(sb, sm, Sensor.TYPE_GYROSCOPE, "Gyroscope");
        appendSensor(sb, sm, Sensor.TYPE_ROTATION_VECTOR, "Rotation Vector");
        appendSensor(sb, sm, Sensor.TYPE_MAGNETIC_FIELD, "Magnetometer");
        appendSensor(sb, sm, Sensor.TYPE_PRESSURE, "Barometer");
        sensorView.setText(sb.toString());
    }

    private void appendSensor(StringBuilder sb, SensorManager sm, int type, String label) {
        Sensor s = sm.getDefaultSensor(type);
        sb.append(label).append(": ").append(s == null ? "NO" : "YES · " + s.getName()).append('\n');
    }

    private void saveSettings() {
        getSharedPreferences(LoggerService.PREFS, MODE_PRIVATE).edit()
                .putString(LoggerService.PREF_ENDPOINT, endpointEdit.getText().toString().trim())
                .putString(LoggerService.PREF_TOKEN, tokenEdit.getText().toString().trim())
                .apply();
    }

    private void loadSettings() {
        SharedPreferences p = getSharedPreferences(LoggerService.PREFS, MODE_PRIVATE);
        endpointEdit.setText(p.getString(LoggerService.PREF_ENDPOINT, ""));
        tokenEdit.setText(p.getString(LoggerService.PREF_TOKEN, ""));
    }

    private void exportDatabase() {
        try {
            SignalDatabase helper = new SignalDatabase(this);
            helper.getWritableDatabase().rawQuery("PRAGMA wal_checkpoint(FULL)", null).close();
            String latest = helper.latestSessionId();
            helper.close();

            File dbFile = getDatabasePath(SignalDatabase.DB_NAME);
            if (!dbFile.exists()) {
                Toast.makeText(this, "No database yet.", Toast.LENGTH_LONG).show();
                return;
            }
            File dir = new File(getExternalFilesDir(Environment.DIRECTORY_DOCUMENTS), "exports");
            if (!dir.exists() && !dir.mkdirs()) throw new IllegalStateException("Cannot create export directory");
            String name = "SignalLogger_" + (latest == null ? System.currentTimeMillis() : latest) + ".signalzip";
            File out = new File(dir, name);
            try (ZipOutputStream zip = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(out)))) {
                zip.putNextEntry(new ZipEntry("session.sqlite"));
                try (BufferedInputStream in = new BufferedInputStream(new FileInputStream(dbFile))) {
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = in.read(buf)) >= 0) zip.write(buf, 0, n);
                }
                zip.closeEntry();
                zip.putNextEntry(new ZipEntry("manifest.json"));
                String manifest = "{\"format\":\"SignalLogger\",\"schemaVersion\":1,\"appVersion\":\"" +
                        "0.1.3" + "\",\"latestSessionId\":\"" +
                        (latest == null ? "" : latest.replace("\"", "")) + "\"}";
                zip.write(manifest.getBytes(StandardCharsets.UTF_8));
                zip.closeEntry();
            }
            Toast.makeText(this, "Exported:\n" + out.getAbsolutePath(), Toast.LENGTH_LONG).show();
        } catch (Exception e) {
            Toast.makeText(this, "Export failed: " + e.getMessage(), Toast.LENGTH_LONG).show();
        }
    }

    private TextView text(String s, int sp, boolean bold) {
        TextView v = new TextView(this);
        v.setText(s);
        v.setTextSize(sp);
        v.setTextColor(Color.BLACK);
        if (bold) v.setTypeface(android.graphics.Typeface.DEFAULT_BOLD);
        return v;
    }

    private Button button(String label) {
        Button b = new Button(this);
        b.setText(label);
        b.setAllCaps(false);
        b.setPadding(dp(8), dp(10), dp(8), dp(10));
        return b;
    }

    private void addSection(LinearLayout root, String title) {
        TextView v = text(title, 13, true);
        v.setTextColor(Color.DKGRAY);
        v.setPadding(0, dp(20), 0, dp(5));
        root.addView(v);
    }

    private LinearLayout.LayoutParams fullWidth() {
        return new LinearLayout.LayoutParams(LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.WRAP_CONTENT);
    }

    private LinearLayout.LayoutParams weighted() {
        LinearLayout.LayoutParams p = new LinearLayout.LayoutParams(0, LinearLayout.LayoutParams.WRAP_CONTENT, 1f);
        p.setMargins(dp(3), 0, dp(3), 0);
        return p;
    }

    private int dp(int value) {
        return Math.round(value * getResources().getDisplayMetrics().density);
    }

    private static String fmt(float v, String suffix) {
        return Float.isNaN(v) ? "--" : String.format(Locale.US, "%.1f %s", v, suffix);
    }

    private static String valueOrDash(String v) {
        return v == null || v.isEmpty() ? "--" : v;
    }

    private static String formatDuration(long ms) {
        long total = ms / 1000;
        long h = total / 3600;
        long m = (total % 3600) / 60;
        long s = total % 60;
        return String.format(Locale.US, "%02d:%02d:%02d", h, m, s);
    }
}
