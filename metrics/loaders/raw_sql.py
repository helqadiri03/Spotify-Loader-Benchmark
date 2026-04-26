"""
Loader 1: Raw SQL (COPY)
Consumes all messages → streams directly into PostgreSQL via COPY FROM STDIN.
Fastest possible approach — minimal Python overhead.
"""
import io
import json
import sys
import time

sys.path.insert(0, "/opt/metrics")
import common

LOADER = "raw_sql"
TABLE  = "playlists_raw_sql"


def run():
    conn     = common.get_pg_conn()
    consumer = common.get_consumer(LOADER)
    kprod    = common.get_kafka_producer()
    reporter = common.MetricsReporter(LOADER)
    reporter.start()

    rows   = 0
    errors = 0
    t0     = time.time()

    # Accumulate CSV in memory, stream to PG via COPY
    buf = io.StringIO()
    for msg in consumer:
        p = msg.value
        try:
            pid          = int(p["pid"])
            name         = (p.get("name") or "").replace('"', '""')
            num_tracks   = int(p.get("num_tracks", 0))
            num_holdouts = int(p.get("num_holdouts", 0))
            num_samples  = int(p.get("num_samples", 0))
            tracks_json  = json.dumps(p.get("tracks", [])).replace('"', '""')
            buf.write(f'{pid},"{name}",{num_tracks},{num_holdouts},{num_samples},"{tracks_json}"\n')
            rows += 1
        except (KeyError, ValueError) as e:
            common.publish_dlq(kprod, LOADER, msg.offset, p, str(e))
            errors += 1
        reporter.update(rows, errors)

    # COPY from the in-memory buffer
    buf.seek(0)
    with conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {TABLE}")
            cur.copy_expert(
                f"COPY {TABLE} (pid, name, num_tracks, num_holdouts, num_samples, tracks) "
                f"FROM STDIN WITH (FORMAT csv, QUOTE '\"')",
                buf,
            )
    conn.close()

    peak_cpu, peak_mem = reporter.stop()
    duration = time.time() - t0
    common.save_result(LOADER, rows, duration, peak_cpu, peak_mem, errors)
    print(f"[{LOADER}] Done: {rows} rows in {duration:.1f}s ({rows/duration:.0f} rps), errors={errors}")


if __name__ == "__main__":
    run()
