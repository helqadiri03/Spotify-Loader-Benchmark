import os
import sys
import psycopg2

PG_DSN = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:6543/benchmark")

def run():
    print("Generating Benchmark Report...")
    try:
        conn = psycopg2.connect(PG_DSN)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT loader, rows_loaded, duration_sec, rows_per_sec, peak_cpu_pct, peak_mem_mb, error_count
                FROM benchmark_results
                ORDER BY duration_sec ASC
            """)
            results = cur.fetchall()
            
        report = "# Spotify Loader Benchmark Results\n\n"
        report += "| Loader | Rows Loaded | Duration (s) | Rows/sec | Peak CPU (%) | Peak Mem (MB) | Errors |\n"
        report += "|---|---|---|---|---|---|---|\n"
        for r in results:
            report += f"| {r[0]} | {r[1]} | {r[2]:.2f} | {r[3]:.0f} | {r[4]:.1f} | {r[5]:.1f} | {r[6]} |\n"
            
        report_path = "/opt/data/benchmark_report.md"
        with open(report_path, "w") as f:
            f.write(report)
            
        print(f"Report generated successfully at {report_path}")
        print(report)
    except Exception as e:
        print(f"Failed to generate report: {e}", file=sys.stderr)

if __name__ == "__main__":
    run()
