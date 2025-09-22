
-- quality_checks.sql
-- 1) Row count must be > 0
SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END AS ok FROM raw.orders;
-- 2) No NULLs in primary key columns
SELECT COUNT(*) AS null_pk FROM raw.orders WHERE order_id IS NULL;
-- 3) Amount must be non-negative
SELECT COUNT(*) AS negative_amounts FROM raw.orders WHERE amount_usd < 0;
-- 4) Status must be within accepted set
SELECT COUNT(*) AS bad_status FROM raw.orders WHERE UPPER(status) NOT IN ('PLACED','SHIPPED','DELIVERED','RETURNED');
