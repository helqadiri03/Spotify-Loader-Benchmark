CREATE KEYSPACE IF NOT EXISTS benchmark
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE benchmark;

CREATE TABLE IF NOT EXISTS strategy_windows (
    strategy     TEXT,
    window_start TIMESTAMP,
    avg_rps      FLOAT,
    peak_cpu     FLOAT,
    avg_mem_mb   FLOAT,
    error_count  INT,
    PRIMARY KEY  (strategy, window_start)
) WITH CLUSTERING ORDER BY (window_start ASC);
