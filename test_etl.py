
import sqlite3, os, subprocess, pathlib

BASE = pathlib.Path(__file__).resolve().parents[1]

def test_pipeline_end_to_end(tmp_path):
    os.chdir(BASE)
    if os.path.exists("demo.db"):
        os.remove("demo.db")
    subprocess.check_call(["python", "etl/ingest_orders.py"])
    subprocess.check_call(["python", "etl/transform_orders.py"])
    subprocess.check_call(["python", "etl/load_orders.py"])
    db = sqlite3.connect("demo.db")
    cur = db.cursor()
    rows = cur.execute("SELECT COUNT(*) FROM mart_daily_revenue").fetchone()[0]
    assert rows >= 1
