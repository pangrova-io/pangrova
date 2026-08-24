-- TPC-DS Q38: Count distinct customers across channels
SELECT COUNT(*) AS customer_count
FROM (
    SELECT DISTINCT c_customer_sk
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
    UNION
    SELECT DISTINCT c_customer_sk
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
    UNION
    SELECT DISTINCT c_customer_sk
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
) customers
