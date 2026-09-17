-- Per-connection settings, which SQLite does not store in the file. Every subscriber connection runs
-- this before init.sql, parsed the same way.

-- First, so connections opening together wait on the schema lock instead of failing on it.
PRAGMA busy_timeout = 10000;

-- SQLite's stock default, set explicitly because esqlite's build lowers it under WAL. FULL fsyncs
-- every commit, as the server databases do on stock config; see finding T in audit.md.
PRAGMA synchronous = FULL;
