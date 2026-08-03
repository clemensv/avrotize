CREATE TABLE telemetry_reading (
    reading_id INTEGER PRIMARY KEY,
    device_id TEXT NOT NULL,
    sensor_type TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT,
    quality INTEGER NOT NULL DEFAULT 100
);

INSERT INTO telemetry_reading
    (reading_id, device_id, sensor_type, recorded_at, value, unit, quality)
VALUES
    (1, 'sensor-042', 'temperature', '2026-08-03T20:00:00Z', 21.7, 'C', 98),
    (2, 'sensor-042', 'humidity', '2026-08-03T20:00:00Z', 44.2, 'percent', 96);
