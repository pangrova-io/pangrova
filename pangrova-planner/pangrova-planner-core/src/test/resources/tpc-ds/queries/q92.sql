-- TPC-DS Q92: Web Sales Large Discounts
SELECT SUM(ws_ext_discount_amt) AS "Excess Discount Amount"
FROM web_sales
JOIN item ON ws_item_sk = i_item_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
WHERE i_manufact_id = 350
  AND d_date BETWEEN DATE '2000-01-27' AND DATE '2000-04-26'
  AND ws_ext_discount_amt > (
      SELECT 1.3 * AVG(ws_ext_discount_amt)
      FROM web_sales
      JOIN item ON ws_item_sk = i_item_sk
      JOIN date_dim ON ws_sold_date_sk = d_date_sk
      WHERE i_manufact_id = 350
        AND d_date BETWEEN DATE '2000-01-27' AND DATE '2000-04-26'
  )
