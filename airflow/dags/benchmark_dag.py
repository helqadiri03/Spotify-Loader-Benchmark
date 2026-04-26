from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import os

default_args = {
    "owner": "benchmark",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def print_results():
    conn = psycopg2.connect(os.getenv("PG_DSN"))
    cur = conn.cursor()
    cur.execute("""
        SELECT loader, rows_loaded,
               ROUND(duration_sec::numeric, 2),
               ROUND(rows_per_sec::numeric, 0),
               peak_cpu_pct, peak_mem_mb, error_count
        FROM benchmark_results
        ORDER BY duration_sec ASC
    """)
    rows = cur.fetchall()
    conn.close()
    print("\n" + "="*70)
    for row in rows:
        print(row)
    print("="*70)

with DAG(
    dag_id="benchmark_dag",
    default_args=default_args,
    description="Full benchmark pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark"],
) as dag:

    truncate = BashOperator(
        task_id="truncate_tables",
        bash_command='psql "$PG_DSN" -c "TRUNCATE raw_records; TRUNCATE loader_results; TRUNCATE bench_events; TRUNCATE benchmark_results;"',
    )

    produce = BashOperator(
        task_id="publish_to_kafka",
        bash_command="DATA_FILE=/opt/data/challenge_set.json KAFKA_BOOTSTRAP_SERVERS=kafka:29092 python /opt/metrics/producer.py",
    )

    loader_sequential = BashOperator(
        task_id="loader_sequential",
        bash_command="PYTHONPATH=/opt/metrics python /opt/metrics/loaders/sequential.py",
    )

    loader_vectorized = BashOperator(
        task_id="loader_vectorized",
        bash_command="PYTHONPATH=/opt/metrics python /opt/metrics/loaders/vectorized.py",
    )

    loader_multithreaded = BashOperator(
        task_id="loader_multithreaded",
        bash_command="PYTHONPATH=/opt/metrics python /opt/metrics/loaders/multithreaded.py",
    )

    loader_raw_sql = BashOperator(
        task_id="loader_raw_sql",
        bash_command="PYTHONPATH=/opt/metrics python /opt/metrics/loaders/raw_sql.py",
    )

    loader_celery = BashOperator(
        task_id="loader_celery",
        bash_command="PYTHONPATH=/opt/metrics python /opt/metrics/loaders/celery_loader.py",
    )

    loader_flink = BashOperator(
        task_id="loader_flink",
        bash_command="PYTHONPATH=/opt/metrics python /opt/metrics/loaders/flink_loader.py",
    )

    results = PythonOperator(
        task_id="print_results",
        python_callable=print_results,
    )

    truncate >> produce >> [
        loader_sequential,
        loader_vectorized,
        loader_multithreaded,
        loader_raw_sql,
        loader_celery,
        loader_flink,
    ] >> results
