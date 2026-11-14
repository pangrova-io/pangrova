-- TPC-DS Q63: Revenue by manager/category
SELECT *
FROM (
    SELECT i_manager_id, SUM(ss_sales_price) AS sum_sales, AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manager_id) AS avg_monthly_sales
    FROM store_sales
    JOIN item ON ss_item_sk = i_item_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 1999
        AND i_category IN ('Books', 'Home', 'Sports')
    GROUP BY i_manager_id, i_category
) sub
WHERE avg_monthly_sales > 0
ORDER BY sum_sales DESC
FETCH FIRST 100 ROWS ONLY
