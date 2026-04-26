-- Playlist tables (one per loader)
CREATE TABLE IF NOT EXISTS playlists_raw_sql (
    pid          INTEGER PRIMARY KEY,
    name         TEXT,
    num_tracks   INTEGER,
    num_holdouts INTEGER,
    num_samples  INTEGER,
    tracks       JSONB,
    loaded_at    TIMESTAMP DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS playlists_sequential    (LIKE playlists_raw_sql INCLUDING ALL);
CREATE TABLE IF NOT EXISTS playlists_vectorized    (LIKE playlists_raw_sql INCLUDING ALL);
CREATE TABLE IF NOT EXISTS playlists_multithreaded (LIKE playlists_raw_sql INCLUDING ALL);
CREATE TABLE IF NOT EXISTS playlists_celery        (LIKE playlists_raw_sql INCLUDING ALL);
CREATE TABLE IF NOT EXISTS playlists_flink         (LIKE playlists_raw_sql INCLUDING ALL);

-- Per-second metrics from each loader (Grafana time-series)
CREATE TABLE IF NOT EXISTS bench_events (
    id          SERIAL PRIMARY KEY,
    strategy    TEXT,
    ts          TIMESTAMP,
    rows_inserted INTEGER,
    throughput_rps FLOAT,
    elapsed_sec FLOAT,
    cpu_pct     FLOAT,
    mem_mb      FLOAT,
    errors      INTEGER,
    status      TEXT
);

-- Final race results (upserted when each loader finishes)
CREATE TABLE IF NOT EXISTS benchmark_results (
    loader          TEXT PRIMARY KEY,
    rows_loaded     INTEGER,
    duration_sec    FLOAT,
    rows_per_sec    FLOAT,
    peak_cpu_pct    FLOAT,
    peak_mem_mb     FLOAT,
    error_count     INTEGER,
    finished_at     TIMESTAMP
);
