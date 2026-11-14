-- TPC-DS Q95: Web Sales Warehouse (With Returns)
WITH ws_wh AS (
    SELECT ws1.ws_order_number, ws1.ws_warehouse_sk AS wh1, ws2.ws_warehouse_sk AS wh2
    FROM web_sales ws1, web_sales ws2
    WHERE ws1.ws_order_number = ws2.ws_order_number
      AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
)
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws_net_profit) AS "total net profit"
FROM web_sales
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN customer_address ON ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws_web_site_sk = web_site_sk
JOIN ws_wh ON web_sales.ws_order_number = ws_wh.ws_order_number
WHERE d_date BETWEEN DATE '1999-02-01' AND DATE '1999-04-02'
  AND ca_state = 'IL'
  AND web_company_name = 'pri'
