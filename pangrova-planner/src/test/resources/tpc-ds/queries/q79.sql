-- TPC-DS Q79: Store Manager Customer Analysis
SELECT c_last_name, c_first_name, s_city, net_paid
FROM (
    SELECT ss_customer_sk, SUM(ss_quantity * ss_list_price) AS net_paid
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    WHERE s_manager_id = 8
    GROUP BY ss_customer_sk
) sales
JOIN customer ON ss_customer_sk = c_customer_sk
JOIN store ON s_store_sk = (SELECT MIN(s_store_sk) FROM store WHERE s_manager_id = 8)
ORDER BY net_paid DESC
FETCH FIRST 100 ROWS ONLY
