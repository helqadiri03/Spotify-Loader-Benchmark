"""
Loader 2: Python Sequential
One message → one INSERT → one commit. Baseline / slowest approach.
"""
import json
import sys
import time

sys.path.insert(0, "/opt/metrics")
import common

LOADER = "sequential"
TABLE  = "playlists_sequential"

INSERT_SQL = f"""
    INSERT INTO {TABLE} (pid, name, num_tracks, num_holdouts, num_samples, tracks)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (pid) DO NOTHING
"""


def run():
    conn     = common.get_pg_conn()
    consumer = common.get_consumer(LOADER)
    kprod    = common.get_kafka_producer()
    reporter = common.MetricsReporter(LOADER)
    reporter.start()

    rows   = 0
    errors = 0
    t0     = time.time()

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {TABLE}")
        conn.commit()

    for msg in consumer:
        p = msg.value
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(INSERT_SQL, (
                        int(p["pid"]),
                        p.get("name"),
                        int(p.get("num_tracks", 0)),
                        int(p.get("num_holdouts", 0)),
                        int(p.get("num_samples", 0)),
                        json.dumps(p.get("tracks", [])),
                    ))
            rows += 1
        except Exception as e:
            common.publish_dlq(kprod, LOADER, msg.offset, p, str(e))
            errors += 1
        reporter.update(rows, errors)

    conn.close()
    peak_cpu, peak_mem = reporter.stop()
    duration = time.time() - t0
    common.save_result(LOADER, rows, duration, peak_cpu, peak_mem, errors)
    print(f"[{LOADER}] Done: {rows} rows in {duration:.1f}s ({rows/duration:.0f} rps), errors={errors}")


if __name__ == "__main__":
    run()
