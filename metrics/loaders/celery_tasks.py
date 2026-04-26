import json
import sys
import os

# We need to ensure we can import common
sys.path.insert(0, "/opt/metrics")
try:
    import common
except ImportError:
    pass # Might fail in some contexts, handled by app if properly set

from celery_app import app

TABLE = "playlists_celery"
INSERT_SQL = f"""
    INSERT INTO {TABLE} (pid, name, num_tracks, num_holdouts, num_samples, tracks)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (pid) DO NOTHING
"""

@app.task(bind=True, max_retries=3)
def load_batch(self, batch_data):
    """
    batch_data is a list of dicts.
    """
    try:
        import common
        conn = common.get_pg_conn()
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
        conn.close()
        return {"rows": len(batch_data), "errors": 0}
    except Exception as exc:
        # Simple retry on failure
        raise self.retry(exc=exc, countdown=2)
