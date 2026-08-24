-- TPC-DS Q32: Excess inventory identification
SELECT SUM(cs_ext_discount_amt) AS excess_discount_amount
FROM catalog_sales
JOIN item ON cs_item_sk = i_item_sk
JOIN date_dim ON cs_sold_date_sk = d_date_sk
WHERE i_manufact_id = 977
    AND d_date BETWEEN DATE '2000-01-27' AND DATE '2000-04-26'
    AND cs_ext_discount_amt > (
        SELECT 1.3 * AVG(cs_ext_discount_amt)
        FROM catalog_sales
        JOIN item ON cs_item_sk = i_item_sk
        JOIN date_dim ON cs_sold_date_sk = d_date_sk
        WHERE i_manufact_id = 977
            AND d_date BETWEEN DATE '2000-01-27' AND DATE '2000-04-26'
    )
FETCH FIRST 100 ROWS ONLY
