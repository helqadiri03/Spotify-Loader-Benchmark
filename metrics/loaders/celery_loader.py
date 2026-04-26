"""
Loader 5: Celery
Main thread consumes Kafka and groups into batches of 500.
Dispatches batches as Celery tasks.
"""
import sys
import time

sys.path.insert(0, "/opt/metrics")
import common
from loaders.celery_tasks import load_batch

LOADER = "celery"
TABLE  = "playlists_celery"
BATCH_SIZE = 500

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
    conn.close()

    batch = []
    async_results = []

    for msg in consumer:
        batch.append(msg.value)
        if len(batch) >= BATCH_SIZE:
            res = load_batch.delay(batch)
            async_results.append(res)
            rows_total += len(batch)
            reporter.update(rows_total, errors_total)
            batch = []

    if batch:
        res = load_batch.delay(batch)
        async_results.append(res)
        rows_total += len(batch)

    # Wait for all tasks to complete (blocking)
    print(f"[{LOADER}] Dispatched {len(async_results)} tasks. Waiting for completion...")
    for res in async_results:
        try:
            res_data = res.get() # blocks until task is done
            # Note: We aren't dynamically updating rows based on actual success in real time here,
            # we assume dispatched = completed for simplicity of the reporter.
        except Exception as e:
            errors_total += BATCH_SIZE
            print(f"Task failed: {e}", file=sys.stderr)

    reporter.update(rows_total, errors_total)
    peak_cpu, peak_mem = reporter.stop()
    duration = time.time() - t0
    
    common.save_result(LOADER, rows_total, duration, peak_cpu, peak_mem, errors_total)
    print(f"[{LOADER}] Done: {rows_total} rows in {duration:.1f}s ({rows_total/duration:.0f} rps), errors={errors_total}")

if __name__ == "__main__":
    run()
