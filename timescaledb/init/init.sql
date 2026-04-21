CREATE TABLE IF NOT EXISTS Data (
    DeviceName TEXT NOT NULL,
    Value INTEGER NOT NULL,
    Timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('Data', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS sensor_status (
    DeviceName TEXT NOT NULL,
    Status TEXT NOT NULL CHECK (Status IN ('ALIVE', 'MISSING')),
    reportedat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('sensor_status', 'reportedat', if_not_exists => TRUE);
