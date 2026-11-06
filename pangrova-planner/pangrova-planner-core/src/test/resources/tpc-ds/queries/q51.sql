-- TPC-DS Q51: Cumulative web/store sales
WITH web_v1 AS (
    SELECT ws_item_sk AS item_sk, d_date,
        SUM(ws_sales_price) AS sum_sales,
        SUM(SUM(ws_sales_price)) OVER (ORDER BY d_date ROWS UNBOUNDED PRECEDING) AS cumulative_sales
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
    GROUP BY ws_item_sk, d_date
),
store_v1 AS (
    SELECT ss_item_sk AS item_sk, d_date,
        SUM(ss_sales_price) AS sum_sales,
        SUM(SUM(ss_sales_price)) OVER (ORDER BY d_date ROWS UNBOUNDED PRECEDING) AS cumulative_sales
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
    GROUP BY ss_item_sk, d_date
)
SELECT web.item_sk, web.d_date, web.sum_sales AS web_sales, web.cumulative_sales AS web_cumulative,
    store.sum_sales AS store_sales, store.cumulative_sales AS store_cumulative
FROM web_v1 web
JOIN store_v1 store ON web.item_sk = store.item_sk AND web.d_date = store.d_date
WHERE web.d_date BETWEEN DATE '2000-01-03' AND DATE '2000-02-02'
ORDER BY web.item_sk, web.d_date
FETCH FIRST 100 ROWS ONLY
