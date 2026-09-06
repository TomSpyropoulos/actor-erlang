-- MySQL schema, the counterpart to timescaledb/init/init.sql. Table and column names match it
-- exactly so the subscriber issues the same statements against either database.
--
-- Table names are case-sensitive on Linux MySQL (lower_case_table_names=0), so `Data` here means
-- every query -- application and smoke test alike -- must also say `Data`, not `data`.

CREATE TABLE IF NOT EXISTS Data (
    DeviceName VARCHAR(255) NOT NULL,
    Value INT NOT NULL,
    -- DATETIME(6), never bare DATETIME: bare defaults to second precision and would silently
    -- truncate the microseconds the wire format carries, which every e2e latency depends on.
    Timestamp DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    -- TimescaleDB gets time-based chunk exclusion from create_hypertable; InnoDB has no equivalent.
    -- Without this index the read group's bounded-window query degrades to a full scan of a table
    -- that grows all run, so read latency would drift upward with elapsed time -- exactly what the
    -- bounded window in db_read_backend_mysql.erl was written to prevent.
    INDEX idx_data_timestamp (Timestamp)
);

CREATE TABLE IF NOT EXISTS sensor_status (
    DeviceName VARCHAR(255) NOT NULL,
    Status VARCHAR(16) NOT NULL CHECK (Status IN ('ALIVE', 'MISSING')),
    reportedat DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    INDEX idx_sensor_status_reportedat (reportedat)
);
