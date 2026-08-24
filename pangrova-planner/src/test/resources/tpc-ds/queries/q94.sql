-- TPC-DS Q94: Web Sales Warehouse (No Returns)
SELECT COUNT(DISTINCT ws_order_number) AS "order count",
       SUM(ws_ext_ship_cost) AS "total shipping cost",
       SUM(ws_net_profit) AS "total net profit"
FROM web_sales ws1
JOIN date_dim ON ws_sold_date_sk = d_date_sk
JOIN customer_address ON ws_ship_addr_sk = ca_address_sk
JOIN web_site ON ws_web_site_sk = web_site_sk
WHERE d_date BETWEEN DATE '1999-02-01' AND DATE '1999-04-02'
  AND ca_state = 'IL'
  AND web_company_name = 'pri'
  AND EXISTS (
      SELECT * FROM web_sales ws2
      WHERE ws1.ws_order_number = ws2.ws_order_number
        AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk
  )
  AND NOT EXISTS (
      SELECT * FROM web_returns wr1
      WHERE ws1.ws_order_number = wr1.wr_order_number
  )
