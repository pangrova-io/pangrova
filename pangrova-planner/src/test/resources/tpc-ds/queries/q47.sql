-- TPC-DS Q47: Monthly store revenue comparison
WITH v1 AS (
    SELECT i_category, i_brand, s_store_name, s_company_name,
        d_year, d_moy, SUM(ss_sales_price) AS sum_sales
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN store ON ss_store_sk = s_store_sk
    JOIN item ON ss_item_sk = i_item_sk
    WHERE d_year IN (1999, 2000)
    GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
),
v2 AS (
    SELECT v1.i_category, v1.i_brand, v1.s_store_name, v1.s_company_name,
        v1.d_year, v1.d_moy, v1.sum_sales,
        LAG(v1.sum_sales, 1) OVER (PARTITION BY v1.i_category, v1.i_brand, v1.s_store_name, v1.s_company_name ORDER BY v1.d_year, v1.d_moy) AS prev_month_sales,
        LEAD(v1.sum_sales, 1) OVER (PARTITION BY v1.i_category, v1.i_brand, v1.s_store_name, v1.s_company_name ORDER BY v1.d_year, v1.d_moy) AS next_month_sales
    FROM v1
)
SELECT v2.i_category, v2.i_brand, v2.s_store_name, v2.s_company_name,
    v2.d_year, v2.d_moy, v2.sum_sales, v2.prev_month_sales, v2.next_month_sales
FROM v2
WHERE v2.d_year = 2000
    AND ((v2.prev_month_sales IS NOT NULL AND v2.sum_sales > v2.prev_month_sales)
        OR (v2.next_month_sales IS NOT NULL AND v2.sum_sales > v2.next_month_sales))
ORDER BY v2.i_category, v2.i_brand, v2.s_store_name, v2.s_company_name, v2.d_year, v2.d_moy
FETCH FIRST 100 ROWS ONLY
