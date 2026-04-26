"""
Loader 6: Flink (Simulated)
Simulates Flink micro-batching. Accumulates messages for a 2-second tumbling window.
On window close, performs a bulk insert.
"""
import json
import sys
import time

sys.path.insert(0, "/opt/metrics")
import common

LOADER = "flink"
TABLE  = "playlists_flink"
WINDOW_SEC = 2.0

INSERT_SQL = f"""
    INSERT INTO {TABLE} (pid, name, num_tracks, num_holdouts, num_samples, tracks)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (pid) DO NOTHING
"""

def flush_window(conn, batch_data):
    if not batch_data:
        return 0, 0
    errors = 0
    try:
        with conn:
            with conn.cursor() as cur:
                data = []
                for p in batch_data:
                    data.append((
                        int(p["pid"]),
                        p.get("name"),
                        int(p.get("num_tracks", 0)),
                        int(p.get("num_holdouts", 0)),
                        int(p.get("num_samples", 0)),
                        json.dumps(p.get("tracks", [])),
                    ))
                cur.executemany(INSERT_SQL, data)
    except Exception as e:
        errors = len(batch_data)
        print(f"Window flush error: {e}", file=sys.stderr)
    return len(batch_data), errors

def run():
    conn     = common.get_pg_conn()
    consumer = common.get_consumer(LOADER)
    kprod    = common.get_kafka_producer()
    reporter = common.MetricsReporter(LOADER)
    reporter.start()

    t0 = time.time()
    rows_total = 0
    errors_total = 0

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {TABLE}")

    window_start = time.time()
    batch = []

    for msg in consumer:
        now = time.time()
        # If window time exceeded, flush
        if now - window_start >= WINDOW_SEC:
            r, e = flush_window(conn, batch)
            rows_total += r
            errors_total += e
            reporter.update(rows_total, errors_total)
            batch = []
            window_start = now
            
        batch.append(msg.value)

    # Flush remaining
    if batch:
        r, e = flush_window(conn, batch)
        rows_total += r
        errors_total += e
        reporter.update(rows_total, errors_total)

    conn.close()
    peak_cpu, peak_mem = reporter.stop()
    duration = time.time() - t0
    
    common.save_result(LOADER, rows_total, duration, peak_cpu, peak_mem, errors_total)
    print(f"[{LOADER}] Done: {rows_total} rows in {duration:.1f}s ({rows_total/duration:.0f} rps), errors={errors_total}")

if __name__ == "__main__":
    run()
