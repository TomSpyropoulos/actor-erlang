CREATE TABLE IF NOT EXISTS Data (
    DeviceName TEXT NOT NULL,
    Value INTEGER NOT NULL,
    Timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('Data', 'timestamp', if_not_exists => TRUE);
