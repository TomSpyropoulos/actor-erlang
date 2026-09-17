-- SQLite schema, the counterpart to timescaledb/init/init.sql and mysql/init/init.sql. Table and
-- column names match them. No server runs this file: every subscriber connection runs it on open,
-- after connection.sql, so each statement must be idempotent.
--
-- Both arms drop whole-line `--` comments and split the rest on semicolons, so no statement may
-- contain one and no comment may share a line with SQL.

-- Stored in the file, so it holds for every later connection. WAL is what lets readers run alongside
-- the single writer.
PRAGMA journal_mode = WAL;

-- STRICT, so a value bound with the wrong type fails instead of being stored as whatever arrived.
-- Timestamp is epoch microseconds, the unit the backend contract already carries. Nothing in the
-- column enforces that unit; see finding U in audit.md.
CREATE TABLE IF NOT EXISTS Data (
    DeviceName TEXT NOT NULL,
    Value INTEGER NOT NULL,
    Timestamp INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
) STRICT;

-- SQLite has no chunk exclusion either, so the read group's bounded window needs this index for the
-- same reason mysql/init/init.sql carries one.
CREATE INDEX IF NOT EXISTS idx_data_timestamp ON Data (Timestamp);

CREATE TABLE IF NOT EXISTS sensor_status (
    DeviceName TEXT NOT NULL,
    Status TEXT NOT NULL CHECK (Status IN ('ALIVE', 'MISSING')),
    reportedat INTEGER NOT NULL DEFAULT (CAST(unixepoch('subsec') * 1000000 AS INTEGER))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_sensor_status_reportedat ON sensor_status (reportedat);
