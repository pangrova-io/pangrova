-- TPC-DS Q87: Exclusive Channel Customers
WITH catalog_customers AS (
    SELECT DISTINCT cs_bill_customer_sk AS customer_sk
    FROM catalog_sales
),
web_customers AS (
    SELECT DISTINCT ws_bill_customer_sk AS customer_sk
    FROM web_sales
),
store_customers AS (
    SELECT DISTINCT ss_customer_sk AS customer_sk
    FROM store_sales
)
SELECT COUNT(*) AS cnt
FROM customer
WHERE c_customer_sk IN (SELECT customer_sk FROM catalog_customers)
  AND c_customer_sk NOT IN (SELECT customer_sk FROM web_customers)
  AND c_customer_sk NOT IN (SELECT customer_sk FROM store_customers)
