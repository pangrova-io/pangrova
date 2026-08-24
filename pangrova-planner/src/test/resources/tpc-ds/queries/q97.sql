-- TPC-DS Q97: Store/Catalog Sales Union
WITH ssci AS (
    SELECT ss_customer_sk AS customer_sk, ss_item_sk AS item_sk
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
    GROUP BY ss_customer_sk, ss_item_sk
),
ccci AS (
    SELECT cs_customer_sk AS customer_sk, cs_item_sk AS item_sk
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_month_seq BETWEEN 1200 AND 1200 + 11
    GROUP BY cs_customer_sk, cs_item_sk
)
SELECT COUNT(*) AS cnt
FROM ssci
FULL OUTER JOIN ccci ON ssci.customer_sk = ccci.customer_sk AND ssci.item_sk = ccci.item_sk
WHERE ssci.customer_sk IS NULL OR ccci.customer_sk IS NULL
