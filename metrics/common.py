"""
common.py — shared utilities for all 6 loaders.
"""
import json
import os
import time
import threading
import datetime
import psutil
import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer, KafkaProducer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PG_DSN         = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:6543/benchmark")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379/0")

SOURCE_TOPIC  = "playlist-raw"
METRICS_TOPIC = "bench-metrics"
DLQ_TOPIC     = "dlq"


# ── Kafka helpers ─────────────────────────────────────────────────────────────
def get_consumer(loader_name: str) -> KafkaConsumer:
    """Consumer with unique group-id so each loader reads from offset 0."""
    run_id = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    return KafkaConsumer(
        SOURCE_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=f"{loader_name}-{run_id}",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=10_000,  # stop when no messages for 10 s
    )


def get_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def publish_metric(producer: KafkaProducer, loader: str, rows: int,
                   elapsed: float, errors: int, status: str = "running") -> None:
    rps = rows / elapsed if elapsed > 0 else 0
    proc = psutil.Process()
    event = {
        "strategy": loader,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "rows_inserted": rows,
        "throughput_rps": round(rps, 2),
        "elapsed_sec": round(elapsed, 2),
        "cpu_pct": psutil.cpu_percent(interval=None),  # system-wide, not per-process
        "mem_mb": round(proc.memory_info().rss / 1_048_576, 1),
        "errors": errors,
        "status": status,
    }
    producer.send(METRICS_TOPIC, value=event)


def publish_dlq(producer: KafkaProducer, loader: str,
                offset: int, record: dict, error: str) -> None:
    event = {
        "strategy": loader,
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "offset": offset,
        "error": error,
        "record": record,
    }
    producer.send(DLQ_TOPIC, value=event)


# ── PostgreSQL helpers ────────────────────────────────────────────────────────
def get_pg_conn():
    return psycopg2.connect(PG_DSN)


def save_result(loader: str, rows: int, duration: float,
                peak_cpu: float, peak_mem: float, errors: int) -> None:
    """Upsert final results into benchmark_results."""
    conn = get_pg_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO benchmark_results
                    (loader, rows_loaded, duration_sec, rows_per_sec,
                     peak_cpu_pct, peak_mem_mb, error_count, finished_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (loader) DO UPDATE SET
                    rows_loaded   = EXCLUDED.rows_loaded,
                    duration_sec  = EXCLUDED.duration_sec,
                    rows_per_sec  = EXCLUDED.rows_per_sec,
                    peak_cpu_pct  = EXCLUDED.peak_cpu_pct,
                    peak_mem_mb   = EXCLUDED.peak_mem_mb,
                    error_count   = EXCLUDED.error_count,
                    finished_at   = EXCLUDED.finished_at
            """, (loader, rows, duration, rows / duration if duration > 0 else 0,
                  peak_cpu, peak_mem, errors))
    conn.close()


# ── Metrics Reporter (background thread) ──────────────────────────────────────
class MetricsReporter(threading.Thread):
    def __init__(self, loader: str):
        super().__init__(daemon=True)
        self.loader = loader
        self.rows = 0
        self.errors = 0
        self.start_time = time.time()
        self._stop_event = threading.Event()
        self._producer = get_kafka_producer()
        self._peak_cpu = 0.0
        self._peak_mem = 0.0
        # prime system-wide CPU counter immediately
        psutil.cpu_percent(interval=None)

    def update(self, rows: int, errors: int = 0) -> None:
        self.rows = rows
        self.errors = errors

    def run(self) -> None:
        while not self._stop_event.is_set():
            # wait 1 second FIRST — this gives psutil a real interval to measure
            self._stop_event.wait(1)
            
            elapsed = time.time() - self.start_time
            
            # system-wide CPU — works correctly in Docker
            cpu = psutil.cpu_percent(interval=None)
            mem = psutil.Process().memory_info().rss / 1_048_576
            
            self._peak_cpu = max(self._peak_cpu, cpu)
            self._peak_mem = max(self._peak_mem, mem)
            
            publish_metric(self._producer, self.loader,
                           self.rows, elapsed, self.errors)

    def stop(self) -> tuple:
        self._stop_event.set()
        elapsed = time.time() - self.start_time
        publish_metric(self._producer, self.loader,
                       self.rows, elapsed, self.errors, status="completed")
        self._producer.flush()
        self._producer.close()
        return self._peak_cpu, self._peak_mem
