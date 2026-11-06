-- TPC-DS Q57: Call center monthly pattern
WITH v1 AS (
    SELECT i_category, i_brand, cc_name, d_year, d_moy,
        SUM(cs_sales_price - COALESCE(cr_refunded_cash, 0)) AS sum_sales
    FROM catalog_sales
    LEFT JOIN catalog_returns ON cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk
    JOIN item ON cs_item_sk = i_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    JOIN call_center ON cs_call_center_sk = cc_call_center_sk
    WHERE d_year IN (1999, 2000)
    GROUP BY i_category, i_brand, cc_name, d_year, d_moy
),
v2 AS (
    SELECT v1.i_category, v1.i_brand, v1.cc_name, v1.d_year, v1.d_moy, v1.sum_sales,
        LAG(v1.sum_sales, 1) OVER (PARTITION BY v1.i_category, v1.i_brand, v1.cc_name ORDER BY v1.d_year, v1.d_moy) AS prev_month_sales,
        LEAD(v1.sum_sales, 1) OVER (PARTITION BY v1.i_category, v1.i_brand, v1.cc_name ORDER BY v1.d_year, v1.d_moy) AS next_month_sales
    FROM v1
)
SELECT v2.i_category, v2.i_brand, v2.cc_name, v2.d_year, v2.d_moy, v2.sum_sales, v2.prev_month_sales, v2.next_month_sales
FROM v2
WHERE v2.d_year = 2000
    AND ((v2.prev_month_sales IS NOT NULL AND v2.sum_sales > v2.prev_month_sales)
        OR (v2.next_month_sales IS NOT NULL AND v2.sum_sales > v2.next_month_sales))
ORDER BY v2.i_category, v2.i_brand, v2.cc_name, v2.d_year, v2.d_moy
FETCH FIRST 100 ROWS ONLY
