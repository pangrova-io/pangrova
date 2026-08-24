-- TPC-DS Q65: Store revenue vs avg per store/item
SELECT s_store_name, i_item_desc, sc.revenue, i_current_price, i_wholesale_cost, i_brand
FROM (
    SELECT ss_store_sk, ss_item_sk, SUM(ss_sales_price) AS revenue
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 2001
        AND d_moy BETWEEN 6 AND 6 + 5
    GROUP BY ss_store_sk, ss_item_sk
) sc
JOIN item ON sc.ss_item_sk = i_item_sk
JOIN store ON sc.ss_store_sk = s_store_sk
WHERE sc.revenue > (
    SELECT AVG(revenue) * 0.1
    FROM (
        SELECT ss_store_sk, ss_item_sk, SUM(ss_sales_price) AS revenue
        FROM store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        WHERE d_year = 2001
            AND d_moy BETWEEN 6 AND 6 + 5
        GROUP BY ss_store_sk, ss_item_sk
    ) avg_revenue
)
ORDER BY s_store_name, i_item_desc
FETCH FIRST 100 ROWS ONLY
