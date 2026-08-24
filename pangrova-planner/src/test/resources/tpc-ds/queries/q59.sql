-- TPC-DS Q59: Weekly store sales vs avg
WITH wss AS (
    SELECT d_week_seq, ss_store_sk, SUM(ss_sales_price) AS sum_sales
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    GROUP BY d_week_seq, ss_store_sk
)
SELECT b.store_name, b.sum_sales / a.sum_sales AS ratio
FROM (
    SELECT d_week_seq, SUM(sum_sales) AS sum_sales
    FROM wss
    WHERE d_week_seq BETWEEN 1200 AND 1200 + 11
    GROUP BY d_week_seq
) a
JOIN (
    SELECT s_store_name, d_week_seq, SUM(sum_sales) AS sum_sales
    FROM wss
    JOIN store ON wss.ss_store_sk = s_store_sk
    WHERE d_week_seq BETWEEN 1200 AND 1200 + 11
    GROUP BY s_store_name, d_week_seq
) b ON a.d_week_seq = b.d_week_seq
ORDER BY b.store_name, ratio
FETCH FIRST 100 ROWS ONLY
