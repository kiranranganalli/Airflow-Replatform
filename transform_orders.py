
"""
Transform raw_orders -> staging_orders with light cleaning
"""
import sqlite3

db = sqlite3.connect("demo.db")
cur = db.cursor()
cur.execute("DROP TABLE IF EXISTS staging_orders")
cur.execute("CREATE TABLE staging_orders AS "
            "SELECT order_id, customer_id, datetime(order_ts) AS order_ts, UPPER(status) AS status, amount_usd "
            "FROM raw_orders")
db.commit()
print("Created staging_orders with", cur.execute("SELECT COUNT(*) FROM staging_orders").fetchone()[0], "rows")
