-- TPC-DS Q85: Web Sales Demographics
SUBSTR(r_reason_desc, 1, 20) AS reason,
       AVG(ws_net_price) AS avg1,
       AVG(ws_list_price) AS avg2,
       AVG(ws_coupon_amt) AS avg3,
       AVG(ws_sales_price) AS avg4
FROM web_sales
JOIN household_demographics ON ws_ship_hdemo_sk = hd_demo_sk
JOIN web_site ON ws_web_site_sk = web_site_sk
JOIN date_dim ON ws_sold_date_sk = d_date_sk
LEFT JOIN web_returns ON ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk
LEFT JOIN reason ON wr_reason_sk = r_reason_sk
WHERE hd_dep_count = 5
  AND web_company_name = 'pri'
  AND d_year = 2000
GROUP BY r_reason_desc
ORDER BY reason, avg1, avg2, avg3, avg4
FETCH FIRST 100 ROWS ONLY
