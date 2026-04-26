"""
Loader 4: Multithreaded
Main thread consumes Kafka and pushes to a queue.
8 worker threads pull batches of 500 from the queue and use executemany to insert.
"""
import json
import sys
import time
import threading
import queue

sys.path.insert(0, "/opt/metrics")
import common

LOADER = "multithreaded"
TABLE  = "playlists_multithreaded"
NUM_THREADS = 8
BATCH_SIZE = 500

INSERT_SQL = f"""
    INSERT INTO {TABLE} (pid, name, num_tracks, num_holdouts, num_samples, tracks)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (pid) DO NOTHING
"""

def worker(q, reporter, result_counter, kprod):
    conn = common.get_pg_conn()
    while True:
        batch = q.get()
        if batch is None:
            break
            
        try:
            with conn:
                with conn.cursor() as cur:
                    # Prepare data tuples
                    data = []
                    for msg in batch:
                        p = msg.value
                        data.append((
                            int(p["pid"]),
                            p.get("name"),
                            int(p.get("num_tracks", 0)),
                            int(p.get("num_holdouts", 0)),
                            int(p.get("num_samples", 0)),
                            json.dumps(p.get("tracks", [])),
                        ))
                    cur.executemany(INSERT_SQL, data)
            
            with result_counter["lock"]:
                result_counter["rows"] += len(batch)
                reporter.update(result_counter["rows"], result_counter["errors"])
                
        except Exception as e:
            with result_counter["lock"]:
                result_counter["errors"] += len(batch)
            print(f"Batch insert error: {e}", file=sys.stderr)
            # We don't individually DLQ here for simplicity on batch failure
            
        finally:
            q.task_done()
            
    conn.close()

def run():
    conn     = common.get_pg_conn()
    consumer = common.get_consumer(LOADER)
    kprod    = common.get_kafka_producer()
    reporter = common.MetricsReporter(LOADER)
    reporter.start()

    t0 = time.time()
    result_counter = {"rows": 0, "errors": 0, "lock": threading.Lock()}

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {TABLE}")
    conn.close()

    q = queue.Queue(maxsize=100) # limit queue size to prevent memory explosion
    threads = []
    
    for _ in range(NUM_THREADS):
        t = threading.Thread(target=worker, args=(q, reporter, result_counter, kprod))
        t.start()
        threads.append(t)

    batch = []
    for msg in consumer:
        batch.append(msg)
        if len(batch) >= BATCH_SIZE:
            q.put(batch)
            batch = []

    if batch:
        q.put(batch)

    # Wait for all tasks to be processed
    q.join()

    # Stop workers
    for _ in range(NUM_THREADS):
        q.put(None)
    for t in threads:
        t.join()

    peak_cpu, peak_mem = reporter.stop()
    duration = time.time() - t0
    
    final_rows = result_counter["rows"]
    final_errors = result_counter["errors"]
    
    common.save_result(LOADER, final_rows, duration, peak_cpu, peak_mem, final_errors)
    print(f"[{LOADER}] Done: {final_rows} rows in {duration:.1f}s ({final_rows/duration:.0f} rps), errors={final_errors}")


if __name__ == "__main__":
    run()
