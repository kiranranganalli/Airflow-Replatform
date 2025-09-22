
"""
Load staging_orders into mart_daily_revenue (upsert by date & segment) using a customers dim table.
"""
import sqlite3

db = sqlite3.connect("demo.db")
cur = db.cursor()
# dim_customers
cur.execute("DROP TABLE IF EXISTS dim_customers")
cur.execute("CREATE TABLE dim_customers (customer_id TEXT PRIMARY KEY, segment TEXT)")
cur.executemany("INSERT INTO dim_customers(customer_id, segment) VALUES (?,?)", [
    ("C-1","Consumer"),("C-2","SMB"),("C-3","Enterprise"),("C-4","Consumer")
])
# target
cur.execute("CREATE TABLE IF NOT EXISTS mart_daily_revenue (ds TEXT, segment TEXT, revenue_usd REAL, orders INTEGER, PRIMARY KEY(ds,segment))")

# compute daily
cur.execute("""
    WITH enriched AS (
        SELECT s.*, d.segment
        FROM staging_orders s
        LEFT JOIN dim_customers d USING(customer_id)
    ),
    daily AS (
        SELECT date(order_ts) AS ds, segment, SUM(amount_usd) AS revenue_usd, COUNT(*) AS orders
        FROM enriched WHERE status IN ('PLACED','DELIVERED','SHIPPED')
        GROUP BY 1,2
    )
    SELECT ds, segment, revenue_usd, orders FROM daily
""")
rows = cur.fetchall()
for ds, segment, revenue, orders in rows:
    cur.execute("INSERT INTO mart_daily_revenue(ds,segment,revenue_usd,orders) VALUES (?,?,?,?) ON CONFLICT(ds,segment) DO UPDATE SET revenue_usd=?, orders=?",
                (ds, segment, revenue, orders, revenue, orders))
db.commit()
print("Upserted", len(rows), "rows into mart_daily_revenue")
