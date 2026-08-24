-- TPC-DS Q54: Store revenue from catalog customers
WITH my_customers AS (
    SELECT DISTINCT c_customer_sk, c_current_addr_sk
    FROM (
        SELECT cs_sold_date_sk AS sold_date_sk, cs_bill_customer_sk AS customer_sk, cs_item_sk AS item_sk
        FROM catalog_sales
        UNION ALL
        SELECT ws_sold_date_sk AS sold_date_sk, ws_bill_customer_sk AS customer_sk, ws_item_sk AS item_sk
        FROM web_sales
    ) cs_or_ws_sales
    JOIN date_dim ON sold_date_sk = d_date_sk
    JOIN customer ON customer_sk = c_customer_sk
    WHERE d_year = 1998
        AND d_moy BETWEEN 4 AND 4 + 2
),
my_revenue AS (
    SELECT c_customer_sk, SUM(ss_ext_sales_price) AS revenue
    FROM my_customers
    JOIN store_sales ON c_customer_sk = ss_customer_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year = 1998
        AND d_moy BETWEEN 4 AND 4 + 2
    GROUP BY c_customer_sk
)
SELECT SUM(revenue) / COUNT(*) AS avg_revenue
FROM my_revenue
