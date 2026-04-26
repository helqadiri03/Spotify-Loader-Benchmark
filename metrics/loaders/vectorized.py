"""
Loader 3: Vectorized
Buffers Kafka messages into a list until 5,000 rows are accumulated.
Then builds a pandas DataFrame and uses df.to_sql(method='multi') for fast inserts.
"""
import json
import sys
import time
import pandas as pd
from sqlalchemy import create_engine

sys.path.insert(0, "/opt/metrics")
import common

LOADER = "vectorized"
TABLE  = "playlists_vectorized"
BATCH_SIZE = 5000

def run():
    conn     = common.get_pg_conn()
    engine   = create_engine(common.PG_DSN)
    consumer = common.get_consumer(LOADER)
    kprod    = common.get_kafka_producer()
    reporter = common.MetricsReporter(LOADER)
    reporter.start()

    rows_total = 0
    errors     = 0
    t0         = time.time()
    batch      = []

    with conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {TABLE}")
    conn.close()

    def flush_batch(batch_list):
        nonlocal rows_total, errors
        if not batch_list:
            return
        
        try:
            df = pd.DataFrame(batch_list)
            # Make sure 'tracks' is dumped as JSON string
            if "tracks" in df.columns:
                df["tracks"] = df["tracks"].apply(lambda x: json.dumps(x) if isinstance(x, list) else json.dumps([]))
            
            # Fill missing columns/NAs if any
            for col in ["num_tracks", "num_holdouts", "num_samples"]:
                if col not in df.columns:
                    df[col] = 0
                else:
                    df[col] = df[col].fillna(0).astype(int)
                    
            if "name" not in df.columns:
                df["name"] = None
                
            # Filter and order columns
            df = df[["pid", "name", "num_tracks", "num_holdouts", "num_samples", "tracks"]]
            
            df.to_sql(TABLE, engine, if_exists="append", index=False, method="multi", chunksize=1000)
            rows_total += len(df)
            
        except Exception as e:
            # If batch fails, we record the error. We can't easily publish DLQ for each row here without looping again,
            # so we just mark the batch size as errors for simplicity, or just 1 error for the batch.
            errors += len(batch_list)
            print(f"Batch flush error: {e}", file=sys.stderr)
            
        finally:
            batch_list.clear()

    for msg in consumer:
        p = msg.value
        try:
            batch.append({
                "pid": int(p["pid"]),
                "name": p.get("name"),
                "num_tracks": p.get("num_tracks", 0),
                "num_holdouts": p.get("num_holdouts", 0),
                "num_samples": p.get("num_samples", 0),
                "tracks": p.get("tracks", []),
            })
            if len(batch) >= BATCH_SIZE:
                flush_batch(batch)
                reporter.update(rows_total, errors)
                
        except Exception as e:
            common.publish_dlq(kprod, LOADER, msg.offset, p, str(e))
            errors += 1
            reporter.update(rows_total, errors)

    # Flush remaining
    flush_batch(batch)
    reporter.update(rows_total, errors)

    peak_cpu, peak_mem = reporter.stop()
    duration = time.time() - t0
    common.save_result(LOADER, rows_total, duration, peak_cpu, peak_mem, errors)
    print(f"[{LOADER}] Done: {rows_total} rows in {duration:.1f}s ({rows_total/duration:.0f} rps), errors={errors}")


if __name__ == "__main__":
    run()
