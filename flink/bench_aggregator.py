"""
Bench Aggregator
Consumes from `bench-metrics` topic continuously.
Aggregates metrics in 10-second tumbling windows per strategy.
Saves to Cassandra and updates PostgreSQL event table.
"""
import json
import time
import os
import datetime
from collections import defaultdict
from kafka import KafkaConsumer
import psycopg2
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KAFKA_BROKER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
PG_DSN         = os.getenv("PG_DSN", "postgresql://postgres:postgres@postgres:5432/benchmark")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
WINDOW_SEC     = 10.0

# ── Cassandra CQL — use ? placeholders, not %s ────────────────────────────────
INSERT_WINDOW_CQL = """
    INSERT INTO strategy_windows
        (strategy, window_start, avg_rps, peak_cpu, avg_mem_mb, error_count)
    VALUES (?, ?, ?, ?, ?, ?)
"""

def get_pg_conn():
    return psycopg2.connect(PG_DSN)

def get_cassandra_session():
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect("benchmark")
    return cluster, session

def aggregate_and_save(windows, session, prepared):
    for strategy, data in windows.items():
        if not data["events"]:
            continue

        events     = data["events"]
        avg_rps    = sum(e["throughput_rps"] for e in events) / len(events)
        peak_cpu   = max(e["cpu_pct"]         for e in events)
        avg_mem_mb = sum(e["mem_mb"]          for e in events) / len(events)
        error_count = events[-1]["errors"]          # cumulative counter

        window_start = datetime.datetime.utcfromtimestamp(data["window_start"])

        # Cassandra: bound statement (? placeholders)
        session.execute(prepared, (
            strategy,
            window_start,
            float(avg_rps),
            float(peak_cpu),
            float(avg_mem_mb),
            int(error_count),
        ))

        data["events"] = []


def run():
    # ── Wait for Cassandra ────────────────────────────────────────────────────
    print("Waiting for Cassandra...")
    cluster = session = prepared = None
    for attempt in range(20):
        try:
            cluster, session = get_cassandra_session()
            prepared = session.prepare(INSERT_WINDOW_CQL)
            print("Cassandra connected.")
            break
        except Exception as exc:
            print(f"  Cassandra not ready ({exc}), retrying in 5 s …")
            time.sleep(5)

    if session is None:
        print("Could not connect to Cassandra after retries — exiting.")
        return

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    pg_conn = None
    for attempt in range(10):
        try:
            pg_conn = get_pg_conn()
            print("PostgreSQL connected.")
            break
        except Exception as exc:
            print(f"  PG not ready ({exc}), retrying in 3 s …")
            time.sleep(3)

    if pg_conn is None:
        print("Could not connect to PostgreSQL — exiting.")
        return

    # ── Kafka consumer ────────────────────────────────────────────────────────
    consumer = KafkaConsumer(
        "bench-metrics",
        bootstrap_servers=KAFKA_BROKER,
        group_id="bench-aggregator",
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    print("Bench Aggregator started — listening on bench-metrics …")

    windows = defaultdict(lambda: {"window_start": time.time(), "events": []})

    for msg in consumer:
        event    = msg.value
        strategy = event["strategy"]
        now      = time.time()

        # flush window every WINDOW_SEC
        if now - windows[strategy]["window_start"] >= WINDOW_SEC:
            try:
                aggregate_and_save({strategy: windows[strategy]}, session, prepared)
            except Exception as exc:
                print(f"Cassandra write error: {exc}")
            windows[strategy]["window_start"] = now

        windows[strategy]["events"].append(event)

        # Write raw event to PostgreSQL for Grafana live chart
        try:
            with pg_conn:
                with pg_conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO bench_events
                            (strategy, ts, rows_inserted, throughput_rps,
                             elapsed_sec, cpu_pct, mem_mb, errors, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            strategy,
                            event["timestamp"],
                            event["rows_inserted"],
                            event["throughput_rps"],
                            event["elapsed_sec"],
                            event["cpu_pct"],
                            event["mem_mb"],
                            event["errors"],
                            event["status"],
                        ),
                    )
        except Exception as exc:
            print(f"PG insert error: {exc}")
            # reconnect on broken connection
            try:
                pg_conn = get_pg_conn()
            except Exception:
                pass


if __name__ == "__main__":
    run()
