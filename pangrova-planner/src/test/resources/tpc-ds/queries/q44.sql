-- TPC-DS Q44: Best/worst store items by profit
SELECT rnk, i1.i_product_name AS best_performing, i2.i_product_name AS worst_performing
FROM (
    SELECT i_item_sk, RANK() OVER (ORDER BY profit DESC) AS rnk
    FROM (
        SELECT ss_item_sk, SUM(ss_ext_sales_price - ss_ext_cost) AS profit
        FROM store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        JOIN store ON ss_store_sk = s_store_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
            AND s_state = 'TN'
        GROUP BY ss_item_sk
    ) sub1
) i1
JOIN (
    SELECT i_item_sk, RANK() OVER (ORDER BY profit ASC) AS rnk
    FROM (
        SELECT ss_item_sk, SUM(ss_ext_sales_price - ss_ext_cost) AS profit
        FROM store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        JOIN store ON ss_store_sk = s_store_sk
        JOIN item ON ss_item_sk = i_item_sk
        WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
            AND s_state = 'TN'
        GROUP BY ss_item_sk
    ) sub2
) i2 ON i1.rnk = i2.rnk
WHERE i1.rnk <= 10
ORDER BY rnk
FETCH FIRST 100 ROWS ONLY
