-- TPC-DS Q78: Cross-Channel Repeat Customers
WITH ws AS (
    SELECT d_year AS ws_year, ws_bill_customer_sk AS customer_sk,
           SUM(ws_ext_sales_price) AS ws_year_sales
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 2000 AND 2001
    GROUP BY d_year, ws_bill_customer_sk
),
cs AS (
    SELECT d_year AS cs_year, cs_bill_customer_sk AS customer_sk,
           SUM(cs_ext_sales_price) AS cs_year_sales
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 2000 AND 2001
    GROUP BY d_year, cs_bill_customer_sk
),
ss AS (
    SELECT d_year AS ss_year, ss_customer_sk AS customer_sk,
           SUM(ss_ext_sales_price) AS ss_year_sales
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 2000 AND 2001
    GROUP BY d_year, ss_customer_sk
)
SELECT c_last_name, c_first_name, c_salutation, c_preferred_cust_flag
FROM customer
JOIN (
    SELECT ws1.customer_sk
    FROM ws ws1, ws ws2, cs cs1, cs cs2, ss ss1, ss ss2
    WHERE ws1.customer_sk = ws2.customer_sk
      AND ws1.customer_sk = cs1.customer_sk
      AND ws1.customer_sk = cs2.customer_sk
      AND ws1.customer_sk = ss1.customer_sk
      AND ws1.customer_sk = ss2.customer_sk
      AND ws1.ws_year = 2000
      AND ws2.ws_year = 2001
      AND cs1.cs_year = 2000
      AND cs2.cs_year = 2001
      AND ss1.ss_year = 2000
      AND ss2.ss_year = 2001
      AND ws1.ws_year_sales > 0
      AND ws2.ws_year_sales > 0
      AND cs1.cs_year_sales > 0
      AND cs2.cs_year_sales > 0
      AND ss1.ss_year_sales > 0
      AND ss2.ss_year_sales > 0
      AND CASE WHEN ws1.ws_year_sales > 0 THEN ws2.ws_year_sales / ws1.ws_year_sales ELSE NULL END
        > CASE WHEN cs1.cs_year_sales > 0 THEN cs2.cs_year_sales / cs1.cs_year_sales ELSE NULL END
      AND CASE WHEN cs1.cs_year_sales > 0 THEN cs2.cs_year_sales / cs1.cs_year_sales ELSE NULL END
        > CASE WHEN ss1.ss_year_sales > 0 THEN ss2.ss_year_sales / ss1.ss_year_sales ELSE NULL END
) best_customers ON customer_sk = c_customer_sk
ORDER BY c_last_name, c_first_name, c_salutation, c_preferred_cust_flag
FETCH FIRST 100 ROWS ONLY
