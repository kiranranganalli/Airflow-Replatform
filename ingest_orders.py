
"""
Ingest sample CSVs into a local SQLite database to simulate a 'raw' layer.
"""
import sqlite3, csv, pathlib

db = sqlite3.connect("demo.db")
cur = db.cursor()
cur.execute("DROP TABLE IF EXISTS raw_orders")
cur.execute("CREATE TABLE raw_orders (order_id TEXT PRIMARY KEY, customer_id TEXT, order_ts TEXT, status TEXT, amount_usd REAL)")

csv_path = pathlib.Path("datasets/orders.csv")
with open(csv_path, newline="") as f:
    for row in csv.DictReader(f):
        cur.execute(
            "INSERT INTO raw_orders(order_id, customer_id, order_ts, status, amount_usd) VALUES (?,?,?,?,?)",
            (row["order_id"], row["customer_id"], row["order_ts"], row["status"], float(row["amount_usd"]))
        )
db.commit()
print("Loaded", cur.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0], "rows into raw_orders")
